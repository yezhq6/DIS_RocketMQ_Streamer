import json
from typing import Optional
from rocketmq.client import Producer, Message

class MissionController:
    """
    Mission control class for sending start/stop signals and managing mission state
    """

    def __init__(self, namesrv_addr: str = "127.0.0.1:9876", topic: str = "DIS_TOPIC"):
        self.namesrv_addr = namesrv_addr
        self.topic = topic
        self.producer: Optional[Producer] = None
        self._setup_producer()

    def _setup_producer(self) -> None:
        """Initialize RocketMQ producer for mission control"""
        self.producer = Producer('MISSION_CONTROL_PRODUCER')
        self.producer.set_name_server_address(self.namesrv_addr)
        self.producer.start()
        print(f"[MissionController] Mission control producer started")

    def send_mission_start(self, mission_id: int = 1001, mission_name: str = "DIS_PRODUCER_MISSION") -> bool:
        """
        Send mission start signal with timestamp
        """
        if not self.producer:
            print("[MissionController] Producer not initialized")
            return False

        try:
            # Create start message
            msg = Message(self.topic)
            msg.set_tags('MISSION_START')
            msg.set_keys('mission_start')

            # Prepare message body with current timestamp
            timestamp = 1756800299

            msg_body = {
                "MissionState": 1,
                "MissionID": mission_id,
                "MissionName": mission_name,
                "PlatformInfoFileName": "PLATFROM_TEST",
                "TimeStamp": timestamp
            }

            msg.set_body(json.dumps(msg_body, ensure_ascii=False, indent=0))

            # Send message
            result = self.producer.send_sync(msg)
            if result.status == 0:
                print(
                    f"[MissionController] Mission start signal sent successfully | MissionID: {mission_id} | TimeStamp: {timestamp}")
                return True
            else:
                print(f"[MissionController] Failed to send mission start signal | Status: {result.status}")
                return False

        except Exception as e:
            print(f"[MissionController] Exception when sending mission start: {e}")
            return False

    def send_mission_stop(self, mission_id: int = 1001) -> bool:
        """
        Send mission stop signal
        """
        if not self.producer:
            print("[MissionController] Producer not initialized")
            return False

        try:
            # Create stop message
            msg = Message(self.topic)
            msg.set_tags('MISSION_STOP')
            msg.set_keys('mission_stop')

            # Prepare message body
            timestamp = 4238544549

            msg_body = {
                "MissionState": 0,  # 0 indicates stopped
                "MissionID": mission_id,
                "MissionName": "DIS_PRODUCER_MISSION",
                "StopReason": "No DIS data received for timeout period",
                "TimeStamp": timestamp
            }

            msg.set_body(json.dumps(msg_body, ensure_ascii=False, indent=0))

            # Send message
            result = self.producer.send_sync(msg)
            if result.status == 0:
                print(
                    f"[MissionController] Mission stop signal sent successfully | MissionID: {mission_id} | TimeStamp: {timestamp}")
                return True
            else:
                print(f"[MissionController] Failed to send mission stop signal | Status: {result.status}")
                return False

        except Exception as e:
            print(f"[MissionController] Exception when sending mission stop: {e}")
            return False

    def shutdown(self) -> None:
        """Shutdown the mission control producer"""
        if self.producer:
            self.producer.shutdown()
            print("[MissionController] Mission control producer shutdown")