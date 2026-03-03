import socket
import time
from dis_rocketmq.dis.receiver import DISReceiver
from dis_rocketmq.rocketmq.sender import RocketMQSender
from dis_rocketmq.controller.mission_controller import MissionController
from dis_rocketmq.file.entities import extract_entities_from_file
from dis_rocketmq.config import create_argparser, print_config
from dis_rocketmq.stats import update_statistics, print_statistics

# ------------------------------ 集中配置参数 ------------------------------
DEFAULT_CONFIG = {
    'udp_ip': '0.0.0.0',  # DIS_ROCKETMQ_codes所在IP
    'udp_port': 60241,
    'rocketmq_namesrv': '10.10.20.15:19876',  # 新配置的WSL虚拟网卡IP，能成功连接
    'rocketmq_topic': 'BattlefieldDeductionSimulation',
    'timeout_seconds': 60 * 0.5,  # 超时时间设置为30秒
    'rocketmq_topic_controller': 'BattlefieldDeductionSimulation',
    'tag': 'PositionEvent'
}

# ------------------------------ 参数解析 ------------------------------
def parse_args():
    config_args = [
        ('', '--udp-ip', str, 'udp_ip', f'UDP IP address'),
        ('', '--udp-port', int, 'udp_port', f'UDP port'),
        ('', '--rocketmq-namesrv', str, 'rocketmq_namesrv', f'RocketMQ NameServer address'),
        ('', '--rocketmq-topic', str, 'rocketmq_topic', f'RocketMQ topic'),
        ('', '--timeout-seconds', float, 'timeout_seconds', f'Timeout seconds'),
        ('', '--rocketmq-topic-controller', str, 'rocketmq_topic_controller', f'RocketMQ topic for controller'),
        ('', '--tag', str, 'tag', f'Message tag')
    ]
    parser = create_argparser('DIS to RocketMQ Producer', DEFAULT_CONFIG, config_args)
    return parser.parse_args()

# ------------------------------ 主程序（协调两个类）------------------------------
def main(config):
    # 从配置字典中提取参数
    UDP_IP = config['udp_ip']
    UDP_PORT = config['udp_port']
    ROCKETMQ_NAMESRV = config['rocketmq_namesrv']
    ROCKETMQ_TOPIC = config['rocketmq_topic']
    TIMEOUT_SECONDS = config['timeout_seconds']
    ROCKETMQ_TOPIC_CONTROLLER = config['rocketmq_topic_controller']
    tag = config['tag']

    # 初始化核心类
    dis_receiver = DISReceiver(udp_ip=UDP_IP, udp_port=UDP_PORT)
    rocket_sender = RocketMQSender(namesrv_addr=ROCKETMQ_NAMESRV, topic=ROCKETMQ_TOPIC)
    mission_controller = MissionController(namesrv_addr=ROCKETMQ_NAMESRV, topic=ROCKETMQ_TOPIC_CONTROLLER)

    print("\n[主程序] DIS -> RocketMQ 桥接服务已启动\n")
    i = 0
    statistics = {}
    last_receive_time = time.time()  # 记录最后一次接收到数据的时间

    try:
        # 主循环：接收 -> 解析 -> 发送
        while True:
            # 设置 socket 超时以便定期检查是否超时
            dis_receiver.udp_socket.settimeout(5.0)  # 每5秒检查一次是否超时
            try:
                # 1. 接收 DIS 数据
                data, addr = dis_receiver.receive_dis_packet()
                last_receive_time = time.time()  # 更新最后接收时间

                # 2. 解析数据（仅保留 EntityStatePdu 的结构化信息）
                entity_pdu, process_msg = dis_receiver.process_received_data(data, addr)
                
                # 打印处理消息
                if process_msg:
                    print(process_msg)

                # 3. 发送到 RocketMQ（解析成功才发送）
                if entity_pdu:
                    # 使用单向发送，不等待确认，提高发送速度
                    success = rocket_sender.send_oneway(entity_pdu, tags=tag, keys=str(entity_pdu["node_id"]))
                    if success:
                        print(f"[主程序] 发送成功 | 实体ID: {entity_pdu['node_id']}")
                    else:
                        print(f"[主程序] 发送失败 | 实体ID: {entity_pdu['node_id']}")
                    i += 1
                    update_statistics(statistics, entity_pdu["node_id"])
                    
            except socket.timeout:
                # 检查是否超过设定的超时时间
                if time.time() - last_receive_time > TIMEOUT_SECONDS:
                    print(f"[主程序] 超过 {TIMEOUT_SECONDS} 秒未接收到 DIS 数据，准备结束任务...")
                    break
                continue  # 继续等待数据
                
    except KeyboardInterrupt:
        print("\n[主程序] 收到终止信号，正在优雅关闭...")
    finally:
        # 发送任务结束信号
        mission_controller.send_mission_stop()
        
        # 释放资源
        dis_receiver.close()
        rocket_sender.shutdown()
        mission_controller.shutdown()
        
        # 打印统计信息
        print_statistics(statistics, title="主程序", operation="发送")
        print(f"[主程序] 所有资源已释放，服务停止")


if __name__ == "__main__":
    # 解析命令行参数
    args = parse_args()
    
    # 构建配置字典
    config = {
        'udp_ip': args.udp_ip,
        'udp_port': args.udp_port,
        'rocketmq_namesrv': args.rocketmq_namesrv,
        'rocketmq_topic': args.rocketmq_topic,
        'timeout_seconds': args.timeout_seconds,
        'rocketmq_topic_controller': args.rocketmq_topic_controller,
        'tag': args.tag
    }

    # 打印配置信息
    print_config(config, title="DIS to RocketMQ 桥接服务配置")
    print()

    # 调用主函数
    main(config)