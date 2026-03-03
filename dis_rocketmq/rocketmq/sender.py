import json
from typing import Optional, Dict, Any

# RocketMQ 相关依赖
from rocketmq.client import Producer, Message

class RocketMQSender:
    """
    RocketMQ 消息发送类：专注于 RocketMQ 连接、消息构造与发送，不依赖 DIS 相关逻辑
    """

    def __init__(self, namesrv_addr: str = "127.0.0.1:9876", topic: str = "DIS_TOPIC",
                 producer_group: str = "DIS_PRODUCER_GROUP"):
        self.namesrv_addr = namesrv_addr
        self.topic = topic
        self.producer_group = producer_group
        self.producer: Optional[Producer] = None
        self._setup_producer()

    def _setup_producer(self) -> None:
        """初始化 RocketMQ 生产者"""
        self.producer = Producer(self.producer_group)
        self.producer.set_name_server_address(self.namesrv_addr)  # 正确 API
        self.producer.start()
        print(f"[RocketMQSender] 已启动，连接 NameServer: {self.namesrv_addr}，主题: {self.topic}")

    def send_message(self, message_body: Dict[str, Any], tags: str = "PositionEvent", keys: str = "1") -> tuple[bool, Optional[str]]:
        """
        发送结构化消息到 RocketMQ
        :param message_body: 消息体（字典格式，会自动转为 JSON 字符串）
        :param tags: 消息标签（用于过滤）
        :param keys: 消息关键字（用于查询）
        :return: 发送成功返回 (True, 成功消息字符串)，失败返回 (False, 失败消息字符串)
        """
        if not self.producer:
            error_msg = f"[RocketMQSender] 生产者未初始化，发送失败"
            return False, error_msg

        try:
            # 构造 RocketMQ 消息（JSON 序列化消息体）
            msg = Message(self.topic)
            msg.set_tags(tags)
            msg.set_keys(keys)
            msg.set_body(json.dumps(message_body, ensure_ascii=False, indent=0))  # indent=0 压缩 JSON

            # 同步发送消息
            ret = self.producer.send_sync(msg)

            if ret.status == 0:  # 0 表示发送成功（SEND_OK）
                entity_id = message_body.get("node_id", "未知")
                success_msg = f"[RocketMQSender] 发送成功 | 实体ID: {entity_id}"
                return True, success_msg
            else:
                error_msg = f"[RocketMQSender] 发送失败 | 状态码: {ret.status} | 消息ID: {ret.msg_id}"
                return False, error_msg

        except Exception as e:
            error_msg = f"[RocketMQSender] 发送异常: {e}"
            return False, error_msg

    def send_oneway(self, message_body: Dict[str, Any], tags: str = "PositionEvent", keys: str = "1") -> bool:
        """
        单向发送消息到 RocketMQ（不等待确认，提高发送速度）
        :param message_body: 消息体（字典格式，会自动转为 JSON 字符串）
        :param tags: 消息标签（用于过滤）
        :param keys: 消息关键字（用于查询）
        :return: 发送成功返回 True，失败返回 False
        """
        if not self.producer:
            return False

        try:
            # 构造 RocketMQ 消息（JSON 序列化消息体）
            msg = Message(self.topic)
            msg.set_tags(tags)
            msg.set_keys(keys)
            msg.set_body(json.dumps(message_body, ensure_ascii=False, indent=0))  # indent=0 压缩 JSON

            # 单向发送消息，不等待确认
            self.producer.send_oneway(msg)
            return True

        except Exception as e:
            return False

    def shutdown(self) -> None:
        """关闭 RocketMQ 生产者，释放资源"""
        if self.producer:
            self.producer.shutdown()
            print(f"[RocketMQSender] 生产者已关闭")