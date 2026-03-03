import socket
import json
import time
from dis_rocketmq.dis.receiver import DISReceiver
from dis_rocketmq.file.entities import extract_entities_from_file
from dis_rocketmq.config import create_argparser, print_config
from dis_rocketmq.stats import update_statistics, print_statistics

# ------------------------------ 集中配置参数 ------------------------------
DEFAULT_CONFIG = {
    'udp_ip': '0.0.0.0',  # DIS_ROCKETMQ_codes所在IP
    'udp_port': 60241,
    'jsonl_file': './dis_recorders/nodes_81.jsonl',
    'timeout_seconds': 60 * 0.5  # 超时时间设置为30秒
}

# ------------------------------ 参数解析 ------------------------------
def parse_args():
    config_args = [
        ('', '--udp-ip', str, 'udp_ip', f'UDP IP address'),
        ('', '--udp-port', int, 'udp_port', f'UDP port'),
        ('', '--jsonl-file', str, 'jsonl_file', f'JSONL output file path'),
        ('', '--timeout-seconds', float, 'timeout_seconds', f'Timeout seconds')
    ]
    parser = create_argparser('DIS Data Recorder', DEFAULT_CONFIG, config_args)
    return parser.parse_args()

# ------------------------------ 主程序（协调两个类）------------------------------
def main(config):
    # 从配置字典中提取参数
    UDP_IP = config['udp_ip']
    UDP_PORT = config['udp_port']
    TIMEOUT_SECONDS = config['timeout_seconds']  # 超时时间
    JSONL_FILE = config['jsonl_file']

    # 初始化核心类
    dis_receiver = DISReceiver(udp_ip=UDP_IP, udp_port=UDP_PORT)

    print(f"\n[主程序] DIS 服务已启动, 开始json recorder: {JSONL_FILE}\n")

    i = 0
    statistics = {}
    first_receive_time = 0
    with open(JSONL_FILE, "w") as f:
        last_receive_time = time.time()  # 记录最后一次接收到数据的时间

        try:
            # 主循环：接收 -> 解析 -> 记录
            while True:
                # 设置 socket 超时以便定期检查是否超时
                dis_receiver.udp_socket.settimeout(5.0)  # 每5秒检查一次是否超时
                try:
                    # 1. 接收 DIS 数据
                    data, addr = dis_receiver.receive_dis_packet()
                    if not first_receive_time and data:
                        first_receive_time = time.time()
                    last_receive_time = time.time()  # 更新最后接收时间

                    # 2. 解析数据（仅保留 EntityStatePdu 的结构化信息）
                    entity_pdu, process_msg = dis_receiver.process_received_data(data, addr)
                    
                    # 打印处理消息
                    if process_msg:
                        print(process_msg)

                    # 3. 记录到 JSONL 文件（解析成功才记录）
                    if entity_pdu:
                        node_id = entity_pdu["node_id"]
                        print(f"[主程序] 记录 DIS 数据到 JSONL, node_id: {node_id}")
                        f.write(json.dumps(entity_pdu) + "\n")
                        i += 1
                        # 统计
                        update_statistics(statistics, node_id)
                        
                        
                except socket.timeout:
                    # 检查是否超过设定的超时时间
                    if time.time() - last_receive_time > TIMEOUT_SECONDS:
                        print(f"[主程序] 超过 {TIMEOUT_SECONDS} 秒未接收到 DIS 数据，准备结束任务...")
                        break
                    continue  # 继续等待数据
                    
        except KeyboardInterrupt:
            print("\n[主程序] 收到终止信号，正在优雅关闭...")
        finally: 
            # 释放资源
            dis_receiver.close()
            # 打印统计信息
            print_statistics(statistics, title="主程序", operation="接收")
            print(f"[主程序] 数据传输时间总计：{last_receive_time - first_receive_time:.3f} s")
            print(f"[主程序] 所有资源已释放，服务停止")


if __name__ == "__main__":
    # 解析命令行参数
    args = parse_args()
    
    # 构建配置字典
    config = {
        'udp_ip': args.udp_ip,
        'udp_port': args.udp_port,
        'jsonl_file': args.jsonl_file,
        'timeout_seconds': args.timeout_seconds
    }

    # 打印配置信息
    print_config(config, title="DIS 数据记录器配置")

    # 调用主函数
    main(config)