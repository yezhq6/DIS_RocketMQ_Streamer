from typing import Tuple, Optional, Dict, Any
from dis_rocketmq.file.entities import NodeParser

# DIS 相关依赖
from opendis.dis7 import *
from opendis.RangeCoordinates import GPS, rad2deg

class EntityStatePduDict:
    def __init__(self):
        self.entity_id = None
        self.force_id = None
        self.cluster_head = None
        self.timestamp = None
        self.type = None
        self.node_name = None
        self.position = {}
        self.orientation = {}
        self.velocity = {}
    
    @staticmethod
    def convert_dis_timestamp_to_seconds(timestamp: int) -> Tuple[float, bool]:
        """
        静态方法：将 DIS 32位时间戳转换为小时内的秒数
        :param timestamp: DIS 原始时间戳
        :return: (小时内秒数, 是否同步)
        """
        timestamp_type = timestamp & 0x00000001
        is_synchronized = (timestamp_type == 1)
        timestamp_value = timestamp >> 1
        seconds_in_hour = timestamp_value * (3600.0 / (2 ** 31))
        return seconds_in_hour, is_synchronized
    
    def parse_from_pdu(self, pdu: EntityStatePdu):
        """Parse EntityStatePdu object and populate dictionary fields"""
        if pdu is None:
            return
        
        self.entity_id = pdu.entityID.entityID
        entity_id_container = self.entity_id % 2000
        self.force_id = pdu.forceId
        
        zone_id = self.entity_id // 100
        zone_id_container = zone_id % 20
        id_within_zone = self.entity_id % 100

        # type
        parser = NodeParser()
        result = parser.parse_node_id(entity_id_container)
        _type = result['platform_type']
        # 替换node_name中的id为entity_id
        node_name_list = result['platform_name'].split("_")  # 替换空格为下划线
        node_name_list[-1] = str(self.entity_id)  # 替换最后一个元素为entity_id
        node_name = "_".join(node_name_list)  # 重新组合成字符串
        self.type = _type

        # node_name
        self.node_name = node_name

        # cluster_head
        c4_cluster_head = {
            50: [1, 2],
            51: [3, 4],
            52: [5, 6],
            53: [7, 8],
            54: [9, 10],
            55: [11, 12],
        }

        # 簇头默认为1
        cluster_head = 1

        if zone_id_container > 0:
            if id_within_zone < 1:
                # 看zone_id 在 c4_cluster_head中那一个list中，然后就使用其key作为cluster_head 
                for head, zone_ids in c4_cluster_head.items():
                    if zone_id_container in zone_ids:
                        cluster_head = head
                        break          
            else:
                if id_within_zone < 70:
                    cluster_head = zone_id * 100
                else:
                    cluster_head = zone_id * 100 + 70
        else:
            if ( 10 < id_within_zone and id_within_zone < 27):
                cluster_head = 10
            elif ( 27 < id_within_zone and id_within_zone < 44):
                cluster_head = 27
            else:
                cluster_head = 1

                # 1094单独判断
        if int(self.entity_id) == 1094:
            cluster_head = 1000

        self.cluster_head = cluster_head

        # 时间转换
        seconds_in_hour, is_synchronized = self.convert_dis_timestamp_to_seconds(pdu.timestamp)

        # 转换为绝对时间
        import time
        self.timestamp = time.time()
        
        # 提取 ECEF 坐标和姿态角
        loc = (
            pdu.entityLocation.x,
            pdu.entityLocation.y,
            pdu.entityLocation.z,
            pdu.entityOrientation.psi,
            pdu.entityOrientation.theta,
            pdu.entityOrientation.phi,
        )
        gps = GPS()
        body = gps.ecef2llarpy(*loc)

        self.position= {
            'latitude': rad2deg(body[0]),
            'longitude': rad2deg(body[1]),
            'altitude': body[2],
        }

        self.orientation = {
            'yaw': rad2deg(body[5]),
            'pitch': rad2deg(body[4]),
            'roll': rad2deg(body[3]),
        }

        self.velocity = {
            'v_x': pdu.entityLinearVelocity.x,
            'v_y': pdu.entityLinearVelocity.y,
            'v_z': pdu.entityLinearVelocity.z
        }
    
    def to_dict(self):
        """Convert the EntityStatePduDict to a standard Python dictionary"""
        return {
            "node_id": self.entity_id,
            "side": self.force_id,
            "cluster_head": self.cluster_head,
            "time": self.timestamp,
            "type": self.type,
            "node_name": self.node_name,
            "latitude": self.position["latitude"],
            "longitude": self.position["longitude"],
            "height": self.position["altitude"],
            "angle_yaw": self.orientation["yaw"],
            "angle_pitch": self.orientation["pitch"],
            "angle_roll": self.orientation["roll"],
            "linear_x": self.velocity["v_x"],
            "linear_y": self.velocity["v_y"],
            "linear_z": self.velocity["v_z"],
        }
    
    def __str__(self):
        """String representation of the EntityStatePduDict"""
        return f"EntityStatePduDict(entity_id={self.entity_id}, force_id={self.force_id})"