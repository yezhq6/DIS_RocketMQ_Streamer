import socket
from typing import Tuple, Optional, Dict, Any
from .pdu_parser import EntityStatePduDict

class DISReceiver:
    """
    DIS 数据接收与解析类：专注于 UDP 监听、PDU 接收和解析，不依赖 RocketMQ
    """

    def __init__(self, udp_ip: str = "0.0.0.0", udp_port: int = 60241):
        self.udp_ip = udp_ip
        self.udp_port = udp_port
        self.entity_pdu = EntityStatePduDict()  # 用于坐标转换
        self.udp_socket: Optional[socket.socket] = None
        self._setup_udp_socket()

    def _setup_udp_socket(self) -> None:
        """初始化 UDP  socket，用于监听 DIS 数据"""
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_socket.bind((self.udp_ip, self.udp_port))
        print(f"[DISReceiver] 已启动，监听 DIS 数据 on {self.udp_ip}:{self.udp_port}")

    def receive_dis_packet(self) -> Tuple[bytes, Tuple[str, int]]:
        """
        接收原始 DIS 数据包
        :return: (原始数据字节流, (发送方IP, 发送方端口))
        """
        if not self.udp_socket:
            raise RuntimeError("[DISReceiver] UDP socket 未初始化")
        return self.udp_socket.recvfrom(1024)

    def process_received_data(self, data: bytes, addr: Tuple[str, int]) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        处理接收的原始数据：解析 PDU，返回结构化信息（仅处理 EntityStatePdu）
        :param data: 原始 UDP 数据
        :param addr: 发送方地址 (IP, 端口)
        :return: (结构化实体信息, 处理消息字符串)，非 EntityStatePdu 则返回 (None, 消息字符串)
        """
        try:
            # 解析 PDU
            from opendis.PduFactory import createPdu
            pdu = createPdu(data)
        except Exception as e:
            error_msg = f"[DISReceiver] 解析 PDU 失败（来源: {addr[0]}）: {e}"
            return None, error_msg

        # 仅处理 EntityStatePdu（pduType=1）
        if pdu.pduType == 1:
            try:
                self.entity_pdu.parse_from_pdu(pdu)
                pdu_name = pdu.__class__.__name__
                success_msg = f"[DISReceiver] 收到目标 PDU: {pdu_name}"
                return self.entity_pdu.to_dict(), success_msg
            except Exception as e:
                error_msg = f"[DISReceiver] 解析 EntityStatePdu 失败: {e}"
                return None, error_msg
        else:
            pdu_name = pdu.__class__.__name__
            info_msg = f"[DISReceiver] 收到非目标 PDU: {pdu_name} (类型码: {pdu.pduType})，长度: {len(data)} 字节（来源: {addr[0]}）"
            return None, info_msg

    def close(self) -> None:
        """关闭 UDP socket，释放资源"""
        if self.udp_socket:
            self.udp_socket.close()
            print(f"[DISReceiver] UDP socket 已关闭")