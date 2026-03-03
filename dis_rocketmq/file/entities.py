#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实体文件处理模块，用于从文件中提取实体数据和解析实体ID
"""

import re
import time


def dms_to_decimal(dms_str):
    """将度分秒格式转换为十进制度"""
    direction = dms_str[-1]
    dms = dms_str[:-1].split(':')
    deg = int(dms[0])
    min = int(dms[1])
    sec = float(dms[2])  # 处理小数秒
    decimal = deg + min/60 + sec/3600
    return -decimal if direction in ['s', 'w'] else decimal


class NodeParser:
    """
    NodeID 解析器类
    根据 NodeID 计算平台类型和平台名称
    """

    # 固定 NodeID 映射表
    FIXED_NODE_MAPPINGS = {
        # 红方后备力量
        1: {"platform_type": "C4", "platform_code": "JCP_C4", "platform_name": "JCP_C4_1"},
        2: {"platform_type": "Satellite", "platform_code": "Sat._2", "platform_name": "Sat._2_2"},
        3: {"platform_type": "Satellite", "platform_code": "Sat._3", "platform_name": "Sat._3_3"},
        4: {"platform_type": "C4", "platform_code": "Ship_C4", "platform_name": "Ship_C4_4"},
        5: {"platform_type": "C4", "platform_code": "Ship_C4", "platform_name": "Ship_C4_5"},
        6: {"platform_type": "C4", "platform_code": "Land_C4", "platform_name": "Land_C4_6"},
        7: {"platform_type": "C4", "platform_code": "Land_C4", "platform_name": "Land_C4_7"},
        10: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_10"},
        11: {"platform_type": "PHL", "platform_code": "PHL_11", "platform_name": "PHL_11"},
        12: {"platform_type": "PHL", "platform_code": "PHL_12", "platform_name": "PHL_12"},
        13: {"platform_type": "PHL", "platform_code": "PHL_13", "platform_name": "PHL_13"},
        14: {"platform_type": "PHL", "platform_code": "PHL_14", "platform_name": "PHL_14"},
        15: {"platform_type": "PHL", "platform_code": "PHL_15", "platform_name": "PHL_15"},
        16: {"platform_type": "PHL", "platform_code": "PHL_16", "platform_name": "PHL_16"},
        17: {"platform_type": "PHL", "platform_code": "PHL_17", "platform_name": "PHL_17"},
        18: {"platform_type": "PHL", "platform_code": "PHL_18", "platform_name": "PHL_18"},
        19: {"platform_type": "MLV", "platform_code": "MLV_19", "platform_name": "MLV_19"},
        20: {"platform_type": "MLV", "platform_code": "MLV_20", "platform_name": "MLV_20"},
        21: {"platform_type": "MLV", "platform_code": "MLV_21", "platform_name": "MLV_21"},
        22: {"platform_type": "MLV", "platform_code": "MLV_22", "platform_name": "MLV_22"},
        23: {"platform_type": "MLV", "platform_code": "MLV_23", "platform_name": "MLV_23"},
        24: {"platform_type": "MLV", "platform_code": "MLV_24", "platform_name": "MLV_24"},
        25: {"platform_type": "MLV", "platform_code": "MLV_25", "platform_name": "MLV_25"},
        26: {"platform_type": "MLV", "platform_code": "MLV_26", "platform_name": "MLV_26"},
        27: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_27"},
        28: {"platform_type": "PHL", "platform_code": "PHL_28", "platform_name": "PHL_28"},
        29: {"platform_type": "PHL", "platform_code": "PHL_29", "platform_name": "PHL_29"},
        30: {"platform_type": "PHL", "platform_code": "PHL_30", "platform_name": "PHL_30"},
        31: {"platform_type": "PHL", "platform_code": "PHL_31", "platform_name": "PHL_31"},
        32: {"platform_type": "PHL", "platform_code": "PHL_32", "platform_name": "PHL_32"},
        33: {"platform_type": "PHL", "platform_code": "PHL_33", "platform_name": "PHL_33"},
        34: {"platform_type": "PHL", "platform_code": "PHL_34", "platform_name": "PHL_34"},
        35: {"platform_type": "PHL", "platform_code": "PHL_35", "platform_name": "PHL_35"},
        36: {"platform_type": "MLV", "platform_code": "MLV_36", "platform_name": "MLV_36"},
        37: {"platform_type": "MLV", "platform_code": "MLV_37", "platform_name": "MLV_37"},
        38: {"platform_type": "MLV", "platform_code": "MLV_38", "platform_name": "MLV_38"},
        39: {"platform_type": "MLV", "platform_code": "MLV_39", "platform_name": "MLV_39"},
        40: {"platform_type": "MLV", "platform_code": "MLV_40", "platform_name": "MLV_40"},
        41: {"platform_type": "MLV", "platform_code": "MLV_41", "platform_name": "MLV_41"},
        42: {"platform_type": "MLV", "platform_code": "MLV_42", "platform_name": "MLV_42"},
        43: {"platform_type": "MLV", "platform_code": "MLV_43", "platform_name": "MLV_43"},
        50: {"platform_type": "C4", "platform_code": "Ship_C4", "platform_name": "Ship_C4_50"},
        51: {"platform_type": "C4", "platform_code": "Ship_C4", "platform_name": "Ship_C4_51"},
        52: {"platform_type": "C4", "platform_code": "Ship_C4", "platform_name": "Ship_C4_52"},
        53: {"platform_type": "C4", "platform_code": "Ship_C4", "platform_name": "Ship_C4_53"},
        54: {"platform_type": "C4", "platform_code": "Ship_C4", "platform_name": "Ship_C4_54"},
        55: {"platform_type": "C4", "platform_code": "Ship_C4", "platform_name": "Ship_C4_55"},
        56: {"platform_type": "C4", "platform_code": "Air_C4", "platform_name": "Air_C4_56"},
        57: {"platform_type": "C4", "platform_code": "Air_C4", "platform_name": "Air_C4_57"},
        58: {"platform_type": "C4", "platform_code": "Air_C4", "platform_name": "Air_C4_58"},
        59: {"platform_type": "C4", "platform_code": "Air_C4", "platform_name": "Air_C4_59"},
        60: {"platform_type": "C4", "platform_code": "Air_C4", "platform_name": "Air_C4_60"},
        61: {"platform_type": "C4", "platform_code": "Air_C4", "platform_name": "Air_C4_61"},
    }

    # 区域簇头映射 (100, 200, ..., 1200)
    ZONE_HEAD_MAPPINGS = {
        100: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_100"},
        200: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_200"},
        300: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_300"},
        400: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_400"},
        500: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_500"},
        600: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_600"},
        700: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_700"},
        800: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_800"},
        900: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_900"},
        1000: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_1000"},
        1100: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_1100"},
        1200: {"platform_type": "C4", "platform_code": "Veh_C4", "platform_name": "Veh_C4_1200"},
    }

    # lvc节点
    LVC_NODE_MAPPINGS = {
        1094: {"platform_type": "R", "platform_code": "R", "platform_name": "R_1094"},
        1095: {"platform_type": "U", "platform_code": "U", "platform_name": "U_1095"},
    }

    @classmethod
    def parse_node_id(cls, node_id):
        """
        根据 NodeID 解析平台类型和平台名称
        
        Args:
            node_id (int): 节点 ID
            
        Returns:
            dict: 包含 platform_type, platform_code, platform_name 的字典
        """
        # 首先检查固定映射表
        if node_id in cls.FIXED_NODE_MAPPINGS:
            return cls.FIXED_NODE_MAPPINGS[node_id]

        # 检查是否为区域簇头
        if node_id in cls.ZONE_HEAD_MAPPINGS:
            return cls.ZONE_HEAD_MAPPINGS[node_id]
        
        # 检查是否为lvc节点
        if node_id in cls.LVC_NODE_MAPPINGS:
            return cls.LVC_NODE_MAPPINGS[node_id]

        # 按区域解析
        zone_id = node_id // 100
        id_within_zone = node_id % 100

        # 检查是否为探测区域节点 (100-1299)
        if 1 <= zone_id <= 12:
            base_name = f"Veh_C4_{zone_id}00"  # 默认簇头名称
            
            # 根据簇内序号确定平台类型和名称
            if id_within_zone == 0:
                # 区域簇头
                return {
                    "platform_type": "C4",
                    "platform_code": "Veh_C4",
                    "platform_name": base_name
                }
            elif 1 <= id_within_zone <= 32:
                # 光电探测无人机
                return {
                    "platform_type": "O-UAV",
                    "platform_code": f"O-UAV_{node_id}",
                    "platform_name": f"O-UAV_{node_id}"
                }
            elif 33 <= id_within_zone <= 48:
                # 电子侦察无人机
                return {
                    "platform_type": "R-UAV",
                    "platform_code": f"R-UAV_{node_id}",
                    "platform_name": f"R-UAV_{node_id}"
                }
            elif 49 <= id_within_zone <= 64:
                # 电子干扰无人机
                return {
                    "platform_type": "J-UAV",
                    "platform_code": f"J-UAV_{node_id}",
                    "platform_name": f"J-UAV_{node_id}"
                }
            elif id_within_zone == 70:
                # 爱国者防空系统指挥车
                return {
                    "platform_type": "CV",
                    "platform_code": f"PAC_CV_{node_id}",
                    "platform_name": f"PAC_CV_{node_id}"
                }
            elif id_within_zone == 71:
                # 爱国者防空系统雷达车
                return {
                    "platform_type": "RV",
                    "platform_code": f"PAC_RV_{node_id}",
                    "platform_name": f"PAC_RV_{node_id}"
                }
            elif 72 <= id_within_zone <= 73:
                # 爱国者防空系统导弹发射车
                return {
                    "platform_type": "MLV",
                    "platform_code": f"PAC_MLV_{node_id}",
                    "platform_name": f"PAC_MLV_{node_id}"
                }
            elif id_within_zone == 74:
                # 天剑防空系统指挥车
                return {
                    "platform_type": "CV",
                    "platform_code": f"TC_CV_{node_id}",
                    "platform_name": f"TC_CV_{node_id}"
                }
            elif id_within_zone == 75:
                # 天剑防空系统雷达车
                return {
                    "platform_type": "RV",
                    "platform_code": f"TC_RV_{node_id}",
                    "platform_name": f"TC_RV_{node_id}"
                }
            elif 76 <= id_within_zone <= 77:
                # 天剑防空系统导弹发射车
                return {
                    "platform_type": "MLV",
                    "platform_code": f"TC_MLV_{node_id}",
                    "platform_name": f"TC_MLV_{node_id}"
                }
            elif id_within_zone == 78:
                # 天弓防空系统指挥车
                return {
                    "platform_type": "CV",
                    "platform_code": f"TK_CV_{node_id}",
                    "platform_name": f"TK_CV_{node_id}"
                }
            elif id_within_zone == 79:
                # 天弓防空系统雷达车
                return {
                    "platform_type": "RV",
                    "platform_code": f"TK_RV_{node_id}",
                    "platform_name": f"TK_RV_{node_id}"
                }
            elif 80 <= id_within_zone <= 81:
                # 天弓防空系统导弹发射车
                return {
                    "platform_type": "MLV",
                    "platform_code": f"TK_MLV_{node_id}",
                    "platform_name": f"TK_MLV_{node_id}"
                }
            elif id_within_zone == 90:
                # 反无人机系统1，雷达车
                return {
                    "platform_type": "RV",
                    "platform_code": f"C-UAS_RV_{node_id}",
                    "platform_name": f"C-UAS_RV_{node_id}"
                }
            elif id_within_zone == 91:
                # 反无人机系统1，电子干扰车
                return {
                    "platform_type": "EJ",
                    "platform_code": f"C-UAS_EJ_{node_id}",
                    "platform_name": f"C-UAS_EJ_{node_id}"
                }
            elif id_within_zone == 92:
                # 反无人机系统2，雷达车
                return {
                    "platform_type": "RV",
                    "platform_code": f"C-UAS_RV_{node_id}",
                    "platform_name": f"C-UAS_RV_{node_id}"
                }
            elif id_within_zone == 93:
                # 反无人机系统2，电子干扰车
                return {
                    "platform_type": "EJ",
                    "platform_code": f"C-UAS_EJ_{node_id}",
                    "platform_name": f"C-UAS_EJ_{node_id}"
                }

        # 如果无法匹配，返回默认值
        return {
            "platform_type": "Unknown",
            "platform_code": f"Unknown_{node_id}",
            "platform_name": f"Unknown_{node_id}"
        }


# 解析实体ID函数
parse_entities = NodeParser.parse_node_id


def extract_entities_from_file(file_path):
    """从文件中提取实体数据"""
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    
    entities = {}
    timestamp = time.time()
    
    # 匹配所有platform块（从platform开始到end_platform结束）
    platform_blocks = re.findall(r'platform\s+\d+\s+\w+\s*.*?end_platform', text, re.DOTALL)
    
    for block in platform_blocks:
        # 提取entity_id
        entity_id_match = re.search(r'platform\s+(\d+)\s+\w+', block)
        if not entity_id_match:
            continue
        entity_id = int(entity_id_match.group(1)) # 保留原始整数值
        node_id = entity_id

        # 提取side
        side_match = re.search(r'side\s+(\w+)', block)
        side = 2 if side_match and side_match.group(1).lower() == 'red' else 1

        # type
        # node_name
        zone_id = node_id // 100
        id_within_zone = node_id % 100

        parser = NodeParser()
        result = parser.parse_node_id(entity_id)
        _type = result['platform_type']
        node_name = result['platform_name']

        c4_cluster_head = {
            7: [1, 2],
            8: [3, 4],
        }

        # cluster_head
        if zone_id > 0:
            if id_within_zone < 1:
                # 看zone_id 在 c4_cluster_head中那一个list中，然后就使用其key作为cluster_head 
                for head, zone_ids in c4_cluster_head.items():
                    if zone_id in zone_ids:
                        cluster_head = head
                        break          
            else:
                if id_within_zone < 50:
                    cluster_head = zone_id * 100
                else:
                    cluster_head = zone_id * 100 + 50
        else:
            if ( 10 < id_within_zone and id_within_zone < 27):
                cluster_head = 10
            else:
                cluster_head = 1
        
        # 提取起点位置（第一个position）
        # 修复正则表达式，添加小数点支持
        start_pos_match = re.search(r'\bposition\s+([\d:.]+[ns]\s+[\d:.]+[ew])', block)
        if not start_pos_match:
            # 调试信息：打印无法匹配的块
            print(f"无法匹配位置的块内容：\n{block[:200]}...")
            continue
        start_lat_str, start_lon_str = start_pos_match.group(1).split()
        start_lat = dms_to_decimal(start_lat_str)
        start_lon = dms_to_decimal(start_lon_str)
        
        # 提取高度（默认500ft）
        # altitude_ft = 500 if entity_id % 100 < 64 else 0
        # altitude_m = altitude_ft * 0.3048  # 转换为米

        # 1. 编写正则表达式，捕获altitude的值，考虑不同单位
        altitude_match = re.search(r'altitude\s+(\d+\.?\d*)\s+(m|ft)', block)
        if altitude_match:
            altitude = float(altitude_match.group(1))
            unit = altitude_match.group(2)
            # 如果是英尺，转换为米
            if unit == 'ft':
                altitude *= 0.3048
        else:
            # 默认高度
            altitude = 0.0
        
        # 计算航向角
        yaw = 0
        
        # 构建实体
        entity = {
            "node_id": node_id,
            "side": side,
            "cluster_head": cluster_head,
            "time": timestamp,
            "type": _type,
            "node_name": node_name,
            "latitude": start_lat,
            "longitude": start_lon,
            "height": altitude,
            "angle_yaw": yaw,
            "angle_pitch": 0,
            "angle_roll": 0,
            "linear_x": 0,
            "linear_y": 0,
            "linear_z": 0,
        }
        
        entities[entity["node_id"]] = entity

    # 按照id进行升序排序
    entities = list(entities.values())
    entities = sorted(entities, key=lambda x: x["node_id"])

    # 转换为dict
    entities = {x["node_id"]: x for x in entities}
    
    # 限制到1000个
    # print(f"[主程序] 缓存的实体数量: {len(entities)}")
    
    return entities


# 使用示例和测试函数
if __name__ == "__main__":
    parser = NodeParser()
    
    # 测试一些典型的 NodeID
    test_nodes = [1, 2, 3, 10, 27, 100, 101, 133, 149, 170, 190, 200]
    
    for node_id in test_nodes:
        result = parser.parse_node_id(node_id)
        print(f"NodeID: {node_id:3d} | Type: {result['platform_type']:12s} | "
              f"Code: {result['platform_code']:15s} | Name: {result['platform_name']}")
