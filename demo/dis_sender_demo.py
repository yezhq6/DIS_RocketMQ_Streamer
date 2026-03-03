import socket
import time
from opendis.dis7 import EntityStatePdu, EntityID, Vector3Double, Vector3Float, EulerAngles
from opendis.RangeCoordinates import GPS, deg2rad
from dis_rocketmq.dis.sender import DISSender

# ------------------------------ 配置参数 ------------------------------
REMOTE_UDP_IP = '172.20.80.1'  # 远程UDP目标IP
REMOTE_UDP_PORT = 60240           # 远程UDP目标端口
SEND_INTERVAL = 0.2                # 发送间隔（秒）

# ------------------------------ 经纬度参数 ------------------------------
# 设定目标经纬度和高度
TARGET_LATITUDE = 39.9042  # 北京纬度
TARGET_LONGITUDE = 116.4074  # 北京经度
TARGET_ALTITUDE = 100.0     # 高度（米）

# ------------------------------ 创建并发送PDU ------------------------------
def create_and_send_entity_state_pdu(dis_sender):
    # 创建GPS对象用于坐标转换
    gps = GPS()
    
    # 将经纬度转换为ECEF坐标
    ecef = gps.lla2ecef((TARGET_LATITUDE, TARGET_LONGITUDE, TARGET_ALTITUDE))

    # 创建EntityID对象
    entity_id = EntityID(siteID=22, applicationID=13, entityID=11065)

    # 创建EntityStatePdu对象
    entity_state_pdu = EntityStatePdu(
        entityID=entity_id,
        forceId=2,  # 2表示蓝方
        entityLocation=Vector3Double(x=ecef[0], y=ecef[1], z=ecef[2]),
        entityLinearVelocity=Vector3Float(x=10.0, y=0.0, z=0.0),
        entityOrientation=EulerAngles(psi=0.0, theta=0.0, phi=0.0)
    )

    # 发送PDU
    success, msg = dis_sender.send_pdu(entity_state_pdu)
    print(f"[Demo] 发送结果: {msg}")
    
    if success:
        print(f"[Demo] PDU发送成功! 位置: X={ecef[0]:.2f}, Y={ecef[1]:.2f}, Z={ecef[2]:.2f}米")
    else:
        print("[Demo] PDU发送失败!")

# ------------------------------ 主程序 ------------------------------
if __name__ == "__main__":
    print("[Demo] 开始发送EntityStatePdu...")
    print(f"[Demo] 发送间隔: {SEND_INTERVAL}秒")
    print(f"[Demo] 目标位置: 纬度={TARGET_LATITUDE}, 经度={TARGET_LONGITUDE}, 高度={TARGET_ALTITUDE}米")
    
    # 初始化DISSender
    dis_sender = DISSender(remote_udp_ip=REMOTE_UDP_IP, remote_udp_port=REMOTE_UDP_PORT)
    print("[Demo] 初始化DISSender")

    try:
        # 无限循环发送
        count = 0
        while True:
            count += 1
            print(f"\n[Demo] 第 {count} 次发送")
            create_and_send_entity_state_pdu(dis_sender)
            time.sleep(SEND_INTERVAL)
    except KeyboardInterrupt:
        print("\n[Demo] 收到终止信号，停止发送...")
    finally:
        # 释放资源
        dis_sender.close()
        print("[Demo] 资源已释放")
        print("[Demo] 发送完成!")
