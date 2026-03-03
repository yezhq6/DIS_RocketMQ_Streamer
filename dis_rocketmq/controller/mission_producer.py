#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模拟MQ生产者：从JSONL文件读取数据并发送到RocketMQ
支持暂停、恢复和停止控制
"""

import time
import json
import signal
from typing import Dict, Any, Optional, Generator, Tuple
from dis_rocketmq.rocketmq.sender import RocketMQSender
from dis_rocketmq.config import create_argparser, print_config
from dis_rocketmq.file.jsonl import count_jsonl_lines


# 全局标志，用于控制程序运行状态
is_running = True
is_paused = False


def handle_signal(signum, frame):
    """
    信号处理函数，用于处理各种控制信号
    """
    global is_running, is_paused

    if signum == signal.SIGTERM or signum == signal.SIGINT:
        # 终止信号，不打印任何信息，由web界面统一处理
        is_running = False
    elif signum == signal.SIGUSR1:
        # 暂停信号
        if not is_paused:
            print(f"\n收到暂停信号 {signum}，暂停发送")
            is_paused = True
        else:
            print(f"\n已处于暂停状态，忽略重复暂停信号 {signum}")
    elif signum == signal.SIGUSR2:
        # 恢复信号
        if is_paused:
            print(f"\n收到恢复信号 {signum}，恢复发送")
            is_paused = False
        else:
            print(f"\n已处于运行状态，忽略重复恢复信号 {signum}")


# 注册信号处理函数
signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGUSR1, handle_signal)  # 自定义暂停信号
signal.signal(signal.SIGUSR2, handle_signal)  # 自定义恢复信号


# ------------------------------ 集中配置区域 ------------------------------
# 所有默认参数集中管理，方便统一修改
DEFAULT_CONFIG = {
    'rocketmq_namesrv': '10.10.20.15:19876',  # RocketMQ NameServer地址
    'rocketmq_topic': 'BattlefieldDeductionSimulation',  # RocketMQ主题
    'jsonl_file': './dis_recorders/nodes_over_1k.jsonl',  # JSONL文件路径
    'batch': 100,  # 每批次发送消息数（进度打印间隔）
    'delay': 0.000001,  # 每条消息发送延迟(秒)，确保前端能跟上进度更新
    'tag': 'PositionEvent',  # RocketMQ消息标签
    'time_field': 'time',  # 时间戳字段名
    'speed': 1.0  # 回放速度倍率（1.0表示实时回放）
}


def parse_args():
    """
    解析命令行参数
    """
    config_args = [
        ('', '--rocketmq-namesrv', str, 'rocketmq_namesrv', 'RocketMQ NameServer地址'),
        ('', '--rocketmq-topic', str, 'rocketmq_topic', 'RocketMQ主题'),
        ('', '--jsonl-file', str, 'jsonl_file', 'JSONL文件路径'),
        ('-b', '--batch', int, 'batch', '每批次发送消息数'),
        ('', '--delay', float, 'delay', '每条消息发送延迟(秒)'),
        ('', '--tag', str, 'tag', 'RocketMQ消息标签'),
        ('', '--time-field', str, 'time_field', '时间戳字段名'),
        ('', '--speed', float, 'speed', '回放速度倍率')
    ]
    parser = create_argparser(
        "模拟MQ生产者：从JSONL文件读取数据并发送到RocketMQ",
        DEFAULT_CONFIG, config_args
    )
    return parser.parse_args()


def jsonl_generator(jsonl_file: str, time_field: str = "time") -> Generator[Tuple[int, Dict[str, Any], float], None, None]:
    """
    JSONL文件生成器，逐行返回数据和相对仿真时间

    Args:
        jsonl_file: JSONL文件路径
        time_field: 时间戳字段名

    Yields:
        (行号, 解析后的JSON数据, 相对仿真时间)
    """
    first_timestamp = None

    with open(jsonl_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)

                # 获取当前时间戳
                current_timestamp = data.get(time_field)

                # 计算相对仿真时间
                relative_time = 0.0
                if current_timestamp:
                    if first_timestamp is None:
                        first_timestamp = current_timestamp
                        relative_time = 0.0
                    else:
                        relative_time = current_timestamp - first_timestamp

                yield (line_num, data, relative_time)
            except json.JSONDecodeError:
                print(f"警告：跳过无效JSON行 {line_num}: {line[:100]}...")
                continue
            except Exception as e:
                print(f"警告：跳过处理失败行 {line_num}: {type(e).__name__}: {e}")
                continue


def send_message(producer: RocketMQSender, data: Dict[str, Any], tag: str) -> tuple[bool, str]:
    """
    发送单条消息到RocketMQ

    Args:
        producer: RocketMQ生产者实例
        data: 消息数据
        tag: 消息标签

    Returns:
        (发送成功标志, 结果消息)
    """
    try:
        node_id = str(data.get("node_id", "unknown"))
        # 使用RocketMQSender的send_oneway方法，提高发送速度
        success = producer.send_oneway(
            message_body=data,
            tags=tag,
            keys=node_id
        )
        return success, "发送成功"
    except Exception as e:
        return False, f"发送异常: {str(e)}"


# ------------------------------ 主程序 ------------------------------
def main():
    """
    从JSONL文件读取数据并发送到RocketMQ，支持暂停、恢复和停止控制
    """
    # 解析命令行参数
    args = parse_args()

    # 构建配置字典
    config = {
        'rocketmq_namesrv': args.rocketmq_namesrv,
        'rocketmq_topic': args.rocketmq_topic,
        'jsonl_file': args.jsonl_file,
        'batch': args.batch,
        'delay': args.delay,
        'tag': args.tag,
        'time_field': args.time_field,
        'speed': args.speed
    }

    # 打印配置信息
    print_config(config, title="模拟MQ生产者配置")
    print()

    # 初始化核心类
    rocket_sender: Optional[RocketMQSender] = None
    try:
        rocket_sender = RocketMQSender(namesrv_addr=config['rocketmq_namesrv'], topic=config['rocketmq_topic'])
        print("RocketMQ 服务已启动")
        print()

        # 分析文件信息
        print("正在分析JSONL文件...")
        valid_lines = count_jsonl_lines(config['jsonl_file'])
        print(f"文件分析完成: {valid_lines} 条有效数据")
        print()

        # 开始发送消息
        print("开始发送消息...")
        start_time = time.time()
        pause_start_time = 0
        total_pause_time = 0

        total_count = 0
        success_count = 0
        fail_count = 0

        # 加载所有数据并按时间戳排序
        print("正在加载和排序数据...")
        all_data = []
        first_timestamp = None
        node_ids = set()  # 用于存储唯一的节点ID
        
        with open(config['jsonl_file'], 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    # 获取当前时间戳
                    current_timestamp = data.get(config['time_field'])
                    if current_timestamp:
                        if first_timestamp is None:
                            first_timestamp = current_timestamp
                        all_data.append((current_timestamp, data))
                        # 收集节点ID
                        node_id = data.get("node_id")
                        if node_id:
                            node_ids.add(str(node_id))
                except json.JSONDecodeError:
                    print(f"警告：跳过无效JSON行 {line_num}: {line[:100]}...")
                    continue
                except Exception as e:
                    print(f"警告：跳过处理失败行 {line_num}: {type(e).__name__}: {e}")
                    continue
        
        # 按时间戳排序
        all_data.sort(key=lambda x: x[0])
        total_messages = len(all_data)
        
        if not all_data:
            print("没有有效的数据")
            return
        
        # 记录当前仿真时间
        current_relative_time = 0.0
        base_timestamp = all_data[0][0]
        end_timestamp = all_data[-1][0]
        total_duration = end_timestamp - base_timestamp
        
        # 计算总节点数量
        total_nodes = len(node_ids)
        print(f"数据加载完成: {total_messages} 条有效数据")
        print(f"总节点数量: {total_nodes}")
        print(f"数据时间跨度: {total_duration:.2f}秒")
        print(f"预计发送时长: {total_duration / config['speed']:.2f}秒")
        print()

        # 批量发送配置
        batch_sizes = [500, 100, 50, 10, 5, 1]  # 批量大小序列
        current_batch_index = 2  # 当前批量大小在序列中的索引（初始为50）
        batch_size = batch_sizes[current_batch_index]  # 每批次发送的消息数量
        batch_adjust_threshold = 0.91  # 批量调整阈值，时间比值低于此值时增加批量大小
        batch_decrease_threshold = 1.0  # 批量减少阈值，时间比值超过此值时减少批量大小

        # 发送消息
        i = 0
        while i < total_messages:
            # 检查终止标志
            if not is_running:
                break

            # 检查暂停标志
            if is_paused:
                print(f"任务已暂停，等待恢复信号... | 当前发送消息数: {total_count} | 相对仿真时间: {current_relative_time:.2f}秒")
                # 记录暂停开始时间
                pause_start_time = time.time()
                # 暂停时定期检查恢复信号和终止信号
                while is_paused and is_running:
                    time.sleep(0.5)  # 暂停时每0.5秒检查一次状态
                if not is_running:
                    break
                # 计算暂停时间并更新总暂停时间
                pause_end_time = time.time()
                pause_duration = pause_end_time - pause_start_time
                total_pause_time += pause_duration
                # 更新开始时间，补偿暂停时间
                start_time += pause_duration
                print(f"任务已恢复，等待 {pause_duration:.2f}秒，继续发送消息... | 当前发送消息数: {total_count} | 相对仿真时间: {current_relative_time:.2f}秒")

            # 计算当前批次的消息
            batch_end = min(i + batch_size, total_messages)
            batch_messages = all_data[i:batch_end]
            
            # 获取当前批次第一条消息的时间戳
            batch_first_timestamp = batch_messages[0][0]
            
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
            
            for timestamp, data in batch_messages:
                # 检查终止标志
                if not is_running:
                    break

                # 检查暂停标志
                if is_paused:
                    print(f"任务已暂停，等待恢复信号... | 当前发送消息数: {total_count} | 相对仿真时间: {current_relative_time:.2f}秒")
                    # 记录暂停开始时间
                    pause_start_time = time.time()
                    # 暂停时定期检查恢复信号和终止信号
                    while is_paused and is_running:
                        time.sleep(0.5)  # 暂停时每0.5秒检查一次状态
                    if not is_running:
                        break
                    # 计算暂停时间并更新总暂停时间
                    pause_end_time = time.time()
                    pause_duration = pause_end_time - pause_start_time
                    total_pause_time += pause_duration
                    # 更新开始时间，补偿暂停时间
                    start_time += pause_duration
                    print(f"任务已恢复，等待 {pause_duration:.2f}秒，继续发送消息... | 当前发送消息数: {total_count} | 相对仿真时间: {current_relative_time:.2f}秒")

                total_count += 1
                current_relative_time = timestamp - base_timestamp

                try:
                    # 发送消息
                    success, msg = send_message(rocket_sender, data, config['tag'])

                    if success:
                        batch_success += 1
                    else:
                        batch_fail += 1
                        print(msg)

                except Exception as e:
                    # 简化错误处理，只打印必要信息
                    print(f"处理失败: {type(e).__name__}")
                    batch_fail += 1
            
            # 更新计数
            success_count += batch_success
            fail_count += batch_fail
            
            # 打印进度
            if success_count % config['batch'] == 0 or (i + batch_size) >= total_messages:
                elapsed = time.time() - start_time
                speed = success_count / elapsed if elapsed > 0 else 0
                percent = success_count / total_messages * 100
                # 计算时间进度比值：当前耗时 / 理论耗时
                current_timestamp = batch_messages[-1][0]
                theoretical_elapsed = (current_timestamp - base_timestamp) / config['speed']
                time_ratio = theoretical_elapsed / elapsed if elapsed > 0 else 0
                
                # 打印进度，使用flush=True确保实时输出，格式与app.py解析逻辑匹配
                print(f"{success_count} | 成功: {success_count} | 失败: {fail_count} | 速度: {speed:.1f}条/秒 | 相对仿真时间: {current_relative_time:.2f}秒", flush=True)
                
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
            
            # 更新循环变量
            i += batch_size

        # 检查是否是因为收到终止信号而退出
        if not is_running:
            # 收到终止信号时，不打印任何额外信息，由web界面统一处理状态显示
            pass

        # 结束信息
        end_time = time.time()
        total_elapsed = end_time - start_time

        # 只有在正常完成时才打印"发送完成"信息，收到终止信号时不打印任何信息
        if is_running:
            print()
            print("=== 发送完成 ===")
            print(f"总数据量: {total_count} 条")
            print(f"成功发送: {success_count} 条 ({success_count/total_count*100:.1f}%)")
            print(f"发送失败: {fail_count} 条 ({fail_count/total_count*100:.1f}%)")
            print(f"实际耗时: {total_elapsed:.2f} 秒")
            if total_elapsed > 0:
                print(f"平均速度: {total_count/total_elapsed:.1f} 条/秒")
            print(f"成功率: {success_count/total_count*100:.1f}%")

    except FileNotFoundError:
        print(f"文件 {config['jsonl_file']} 不存在！")
    except Exception as e:
        print(f"主程序执行失败: {type(e).__name__}: {e}")
    finally:
        # 关闭 RocketMQ 生产者
        if rocket_sender:
            rocket_sender.shutdown()
            print("\nRocketMQ 生产者已关闭")


if __name__ == "__main__":
    main()
