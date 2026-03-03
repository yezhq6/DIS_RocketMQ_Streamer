#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
严格时间控制版MQ生产者：从JSONL文件读取数据并基于时间戳发送到RocketMQ
实现效果：JSONL文件记录的是10分钟的仿真，也花了10分钟把数据传出去
"""

import time
import json
import logging
import os
import psutil
from rocketmq.client import Producer, Message
from dis_rocketmq.config import create_argparser, print_config
from dis_rocketmq.file.jsonl import jsonl_generator, analyze_jsonl_file

# 配置日志
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------------------ 集中配置区域 ------------------------------
# 所有配置参数集中管理，方便统一修改
DEFAULT_CONFIG = {
    'rocketmq_namesrv': '10.10.20.15:19876',  # RocketMQ NameServer地址
    'rocketmq_topic': 'BattlefieldDeductionSimulation',  # RocketMQ主题
    'producer_group': 'PRODUCER_GROUP',  # 生产者组名
    'jsonl_file': './dis_recorders/nodes_over_1k.jsonl',  # JSONL文件路径
    'speed': 1.0,  # 回放速度倍率（1.0表示实时回放）
    'progress_interval': 500,  # 进度打印间隔（每多少条消息打印一次）
    'time_field': 'time'  # 时间戳字段名
}


def parse_args():
    """解析命令行参数"""
    config_args = [
        ('', '--rocketmq-namesrv', str, 'rocketmq_namesrv', 'RocketMQ NameServer地址'),
        ('', '--rocketmq-topic', str, 'rocketmq_topic', 'RocketMQ主题'),
        ('', '--producer-group', str, 'producer_group', '生产者组名'),
        ('', '--jsonl-file', str, 'jsonl_file', 'JSONL文件路径'),
        ('', '--speed', float, 'speed', '回放速度倍率'),
        ('', '--progress-interval', int, 'progress_interval', '进度打印间隔'),
        ('', '--time-field', str, 'time_field', '时间戳字段名')
    ]
    parser = create_argparser(
        "严格时间控制版MQ生产者：基于时间戳回放JSONL数据",
        DEFAULT_CONFIG, config_args
    )
    return parser.parse_args()


def send_message(producer, topic, data, max_retries=3):
    """发送单条消息到RocketMQ，支持重试"""
    for retry in range(max_retries):
        try:
            msg = Message(topic)
            msg.set_tags("PositionEvent")
            msg.set_keys(str(data.get("node_id", "unknown")))
            msg.set_body(json.dumps(data, ensure_ascii=False))

            # 使用单向发送，不等待确认，提高发送速度
            producer.send_oneway(msg)
            return True, "发送成功"
        except Exception as e:
            error_msg = f"发送失败 (重试 {retry+1}/{max_retries}): {str(e)}"
            if retry < max_retries - 1:
                logger.warning(error_msg)
                time.sleep(0.1)  # 短暂等待后重试
            else:
                logger.error(error_msg)
                return False, error_msg


def validate_config(config):
    """验证配置参数"""
    if config['speed'] <= 0:
        raise ValueError("回放速度必须大于0")
    if config['progress_interval'] <= 0:
        raise ValueError("进度打印间隔必须大于0")
    if not config['rocketmq_namesrv']:
        raise ValueError("RocketMQ NameServer地址不能为空")
    if not config['rocketmq_topic']:
        raise ValueError("RocketMQ主题不能为空")
    if not config['producer_group']:
        raise ValueError("生产者组名不能为空")
    if not config['jsonl_file']:
        raise ValueError("JSONL文件路径不能为空")
    if not config['time_field']:
        raise ValueError("时间戳字段名不能为空")


def validate_file(file_path):
    """验证文件是否存在且可读"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"文件不存在: {file_path}")
    if not os.path.isfile(file_path):
        raise IsADirectoryError(f"路径不是文件: {file_path}")
    if not os.access(file_path, os.R_OK):
        raise PermissionError(f"文件不可读: {file_path}")


def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()

    # 构建配置字典
    config = {
        'rocketmq_namesrv': args.rocketmq_namesrv,
        'rocketmq_topic': args.rocketmq_topic,
        'producer_group': args.producer_group,
        'jsonl_file': args.jsonl_file,
        'speed': args.speed,
        'progress_interval': args.progress_interval,
        'time_field': args.time_field
    }

    # 验证配置参数
    try:
        validate_config(config)
        validate_file(config['jsonl_file'])
    except (ValueError, FileNotFoundError, IsADirectoryError, PermissionError) as e:
        logger.error(f"验证失败: {str(e)}")
        print(f"错误: {str(e)}")
        return

    # 打印配置信息
    print_config(config, title="严格时间控制版MQ生产者配置")
    print()

    # 分析文件时间信息
    print("正在分析JSONL文件时间信息...")
    time_info = analyze_jsonl_file(config['jsonl_file'], config['time_field'])
    valid_lines = time_info['valid_lines']
    print(f"文件分析完成: {valid_lines} 条有效数据")

    has_time_field = time_info['has_time_field']
    if has_time_field:
        print(f"数据时间跨度: {time_info['time_span']:.2f}秒")
        print(f"预计发送时长: {time_info['time_span'] / config['speed']:.2f}秒")
    else:
        print(f"警告：文件中未找到'{config['time_field']}'字段，将使用固定速度发送")
    print()

    # 初始化RocketMQ生产者
    print("正在初始化RocketMQ生产者...")
    try:
        producer = Producer(config['producer_group'])
        producer.set_name_server_address(config['rocketmq_namesrv'])
        producer.start()
        print("RocketMQ生产者初始化成功")
        print()
    except Exception as e:
        logger.error(f"RocketMQ生产者初始化失败: {str(e)}")
        print(f"RocketMQ生产者初始化失败: {str(e)}")
        return

    # 开始发送
    print("开始发送消息...")
    start_time = time.time()
    success_count = 0
    fail_count = 0

    if has_time_field:
        # 严格基于时间戳的发送逻辑
        # 首先加载所有数据并按时间戳排序
        all_data = []
        for data in jsonl_generator(config['jsonl_file'], config['time_field']):
            if config['time_field'] in data:
                all_data.append(data)
        
        # 按时间戳排序
        all_data.sort(key=lambda x: x[config['time_field']])
        
        if not all_data:
            print("没有有效的数据")
            return
        
        base_timestamp = all_data[0][config['time_field']]
        end_timestamp = all_data[-1][config['time_field']]
        total_duration = end_timestamp - base_timestamp
        
        # 记录开始时间
        current_index = 0
        total_messages = len(all_data)
        
        # 批量发送和异步发送逻辑
        # 记录开始时间
        start_time = time.time()
        
        # 添加调试信息
        # print(f"开始发送: base_timestamp={base_timestamp}, start_time={start_time}")
        # print(f"第一条消息时间戳: {all_data[0][config['time_field']]}")
        # print(f"最后一条消息时间戳: {all_data[-1][config['time_field']]}")
        
        # 批量发送配置
        batch_sizes = [500, 100, 50, 10, 5, 1]  # 批量大小序列
        current_batch_index = 2  # 当前批量大小在序列中的索引（初始为100）
        batch_size = batch_sizes[current_batch_index]  # 每批次发送的消息数量
        batch_adjust_threshold = 0.91  # 批量调整阈值，时间比值低于此值时增加批量大小
        batch_decrease_threshold = 1.0  # 批量减少阈值，时间比值超过此值时减少批量大小
        
        # 使用配置中的打印间隔
        # print(f"使用配置中的progress_interval: {config['progress_interval']}")
        
        # 发送消息
        i = 0
        while i < total_messages:
            # 记录批次开始时间
            batch_start_time = time.time()
            
            # 计算当前批次的消息
            batch_end = min(i + batch_size, total_messages)
            batch_messages = all_data[i:batch_end]
            
            # 获取当前批次第一条消息的时间戳
            batch_first_timestamp = batch_messages[0][config['time_field']]
            
            # 计算理论上应该发送的时间
            theoretical_send_time = start_time + (batch_first_timestamp - base_timestamp) / config['speed']
            
            # 计算当前实际时间
            current_time = time.time()
            
            # 计算需要等待的时间
            wait_time = theoretical_send_time - current_time
            
            # 等待到指定时间
            if wait_time > 0:
                time.sleep(wait_time)
            
            # 批量发送消息
            batch_success = 0
            batch_fail = 0
            
            for msg_data in batch_messages:
                success, result = send_message(producer, config['rocketmq_topic'], msg_data)
                if success:
                    batch_success += 1
                else:
                    batch_fail += 1
                    logger.error(f"发送失败 {i + batch_success + batch_fail}: {result}")
            
            # 更新计数
            success_count += batch_success
            fail_count += batch_fail
            
            # 记录批次处理时间
            batch_end_time = time.time()
            batch_process_time = batch_end_time - batch_start_time
            
            # 打印进度
            if success_count % config['progress_interval'] == 0 or (i + batch_size) >= total_messages:
                elapsed = time.time() - start_time
                speed = success_count / elapsed if elapsed > 0 else 0
                percent = success_count / total_messages * 100
                # 计算时间进度比值：当前耗时 / 理论耗时
                current_timestamp = batch_messages[-1][config['time_field']]
                theoretical_elapsed = (current_timestamp - base_timestamp) / config['speed']
                time_ratio = theoretical_elapsed / elapsed if elapsed > 0 else 0
                print(f"进度: {success_count}/{total_messages} ({percent:.1f}%) | 成功: {success_count} | 失败: {fail_count} | 速度: {speed:.1f}条/秒 | 回放率: {time_ratio:.2f}")
                # print(f"批次大小：{batch_size}")
                
                # 动态调整批量大小
                if time_ratio < batch_adjust_threshold and current_batch_index > 0:
                    # 延迟严重，增加批量大小
                    current_batch_index -= 1
                    new_batch_size = batch_sizes[current_batch_index]
                    batch_size = new_batch_size
                elif time_ratio > batch_decrease_threshold and current_batch_index < len(batch_sizes) - 1:
                    # 发送速度足够，减少批量大小以提高时间控制精度
                    current_batch_index += 1
                    new_batch_size = batch_sizes[current_batch_index]
                    batch_size = new_batch_size
            
            # # 每10批次打印一次调试信息，避免输出过多
            # if i % (batch_size * 10) == 0:
            #     print(f"处理批次完成，当前i: {i}, 批量大小: {batch_size}, 下一批次: {i + batch_size}")
            
            # 更新循环变量
            i += batch_size
    else:
        # 没有时间戳字段，使用固定间隔发送
        # 先计算总数据量
        total_data = []
        for data in jsonl_generator(config['jsonl_file'], config['time_field']):
            total_data.append(data)
        total_messages = len(total_data)
        
        for i, data in enumerate(total_data):
            # 发送消息
            success, result = send_message(producer, config['rocketmq_topic'], data)

            if success:
                success_count += 1
            else:
                fail_count += 1
                print(f"发送失败 {i+1}: {result}")

            # 打印进度
            if (i + 1) % config['progress_interval'] == 0:
                elapsed = time.time() - start_time
                speed = (i + 1) / elapsed if elapsed > 0 else 0
                percent = (i + 1) / total_messages * 100
                print(f"进度: {i+1}/{total_messages} ({percent:.1f}%) | 成功: {success_count} | 失败: {fail_count} | 速度: {speed:.1f}条/秒")

    # 发送完成
    try:
        producer.shutdown()
    except Exception as e:
        logger.error(f"关闭RocketMQ生产者失败: {str(e)}")
    
    total_time = time.time() - start_time
    total_count = success_count + fail_count  # 使用实际发送的数量

    print()
    print("=== 发送完成 ===")
    print(f"总数据量: {total_count} 条")
    print(f"成功发送: {success_count} 条")
    print(f"失败发送: {fail_count} 条")
    print(f"总耗时: {total_time:.2f} 秒")
    if total_time > 0:
        print(f"平均速度: {total_count / total_time:.1f} 条/秒")
    print(f"成功率: {success_count / total_count * 100:.1f}%")



if __name__ == "__main__":
    main()
