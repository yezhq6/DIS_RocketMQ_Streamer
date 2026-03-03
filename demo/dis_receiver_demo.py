import socket
from opendis.PduFactory import createPdu

# ------------------------------ 配置参数 ------------------------------
LOCAL_UDP_IP = '0.0.0.0'  # 本地UDP监听IP
LOCAL_UDP_PORT = 60241     # 本地UDP监听端口

# ------------------------------ 接收并解析PDU ------------------------------
def receive_and_parse_pdu():
    # 创建UDP socket
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_socket.bind((LOCAL_UDP_IP, LOCAL_UDP_PORT))
    print(f"[Demo] 已启动，监听 UDP 数据 on {LOCAL_UDP_IP}:{LOCAL_UDP_PORT}")

    try:
        print("[Demo] 等待接收PDU...")
        while True:
            # 接收数据
            data, addr = udp_socket.recvfrom(1024)
            print(f"[Demo] 接收到数据: 从 {addr[0]}:{addr[1]}，大小: {len(data)} 字节")

            # 解析PDU
            try:
                pdu = createPdu(data)
                print(f"[Demo] 解析成功，PDU类型: {pdu.__class__.__name__}")
                
                # 检查是否为EntityStatePdu
                if hasattr(pdu, 'entityID'):
                    print(f"[Demo] EntityID: siteID={pdu.entityID.siteID}, applicationID={pdu.entityID.applicationID}, entityID={pdu.entityID.entityID}")
                    
                    # 检查是否为我们发送的指定EntityID
                    if pdu.entityID.siteID == 22 and pdu.entityID.applicationID == 13 and pdu.entityID.entityID == 11165:
                        print("[Demo] 找到指定的EntityID! 测试成功!")
                        break
            except Exception as e:
                print(f"[Demo] 解析PDU失败: {e}")
    finally:
        # 关闭socket
        udp_socket.close()
        print("[Demo] UDP socket 已关闭")

# ------------------------------ 主程序 ------------------------------
if __name__ == "__main__":
    print("[Demo] 开始接收EntityStatePdu...")
    receive_and_parse_pdu()
    print("[Demo] 接收完成!")
