import socket
from typing import Optional, Tuple

class DISSender:
    """
    DIS 数据发送类：专注于 UDP 发送 DIS 数据到远程目标
    """

    def __init__(self, remote_udp_ip: str, remote_udp_port: int):
        self.remote_udp_ip = remote_udp_ip
        self.remote_udp_port = remote_udp_port
        self.udp_socket: Optional[socket.socket] = None
        self._setup_udp_socket()

    def _setup_udp_socket(self) -> None:
        """初始化 UDP socket，用于发送 DIS 数据"""
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        print(f"[DISSender] 已启动，准备发送 DIS 数据到 {self.remote_udp_ip}:{self.remote_udp_port}")

    def send_dis_packet(self, data: bytes) -> int:
        """
        发送原始 DIS 数据包到远程目标
        :param data: 原始数据字节流
        :return: 发送的字节数
        """
        if not self.udp_socket:
            raise RuntimeError("[DISSender] UDP socket 未初始化")
        return self.udp_socket.sendto(data, (self.remote_udp_ip, self.remote_udp_port))

    def send_pdu(self, pdu) -> Tuple[bool, str]:
        """
        发送 PDU 对象到远程目标
        :param pdu: PDU 对象
        :return: (是否成功, 消息)
        """
        try:
            # 序列化 PDU 对象为字节流
            from opendis.DataOutputStream import DataOutputStream
            from io import BytesIO
            
            # 第一次序列化，计算长度
            temp_stream = BytesIO()
            temp_output_stream = DataOutputStream(temp_stream)
            pdu.serialize(temp_output_stream)
            pdu_length = len(temp_stream.getvalue())
            
            # 设置PDU长度字段
            pdu.length = pdu_length
            
            # 第二次序列化，使用正确的长度
            output_stream = BytesIO()
            data_output_stream = DataOutputStream(output_stream)
            pdu.serialize(data_output_stream)
            
            # 获取序列化后的字节流
            data = output_stream.getvalue()
            
            bytes_sent = self.send_dis_packet(data)
            success_msg = f"[DISSender] 成功发送 PDU 到 {self.remote_udp_ip}:{self.remote_udp_port}，发送字节数: {bytes_sent}"
            return True, success_msg
        except Exception as e:
            error_msg = f"[DISSender] 发送 PDU 失败: {e}"
            return False, error_msg

    def close(self) -> None:
        """关闭 UDP socket，释放资源"""
        if self.udp_socket:
            self.udp_socket.close()
            print(f"[DISSender] UDP socket 已关闭")
