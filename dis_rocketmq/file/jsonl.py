#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSONL 文件处理模块，用于集中处理 JSONL 文件的读取、分析和生成
"""

import json
from typing import Dict, Any, Generator, Optional, List


def jsonl_generator(file_path: str, time_field: str = "time") -> Generator[Dict[str, Any], None, None]:
    """
    JSONL文件生成器，逐行返回数据
    
    Args:
        file_path: JSONL文件路径
        time_field: 时间戳字段名
    
    Yields:
        解析后的JSON数据
    """
    with open(file_path, 'r') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                yield data
            except json.JSONDecodeError:
                print(f"警告：跳过无效JSON行 {line_num}: {line[:100]}...")
                continue


def analyze_jsonl_file(file_path: str, time_field: str = "time") -> Dict[str, Any]:
    """
    分析JSONL文件，返回文件的统计信息
    
    Args:
        file_path: JSONL文件路径
        time_field: 时间戳字段名
    
    Returns:
        包含文件统计信息的字典
    """
    timestamps = []
    total_lines = 0
    valid_lines = 0
    
    with open(file_path, 'r') as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                valid_lines += 1
                if time_field in data:
                    timestamps.append(data[time_field])
            except json.JSONDecodeError:
                continue
    
    # 计算时间信息
    time_info = {
        'has_time_field': False,
        'min_time': 0.0,
        'max_time': 0.0,
        'time_span': 0.0,
        'total_lines': total_lines,
        'valid_lines': valid_lines
    }
    
    if timestamps:
        time_info['has_time_field'] = True
        time_info['min_time'] = min(timestamps)
        time_info['max_time'] = max(timestamps)
        time_info['time_span'] = time_info['max_time'] - time_info['min_time']
    
    return time_info


def count_jsonl_lines(file_path: str) -> int:
    """
    统计JSONL文件的有效行数
    
    Args:
        file_path: JSONL文件路径
    
    Returns:
        有效行数
    """
    total_lines = 0
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    json.loads(line)
                    total_lines += 1
                except json.JSONDecodeError:
                    continue
    return total_lines


def write_jsonl(file_path: str, data_list: List[Dict[str, Any]]) -> None:
    """
    将数据列表写入JSONL文件
    
    Args:
        file_path: JSONL文件路径
        data_list: 数据列表
    """
    with open(file_path, 'w') as f:
        for data in data_list:
            f.write(json.dumps(data) + '\n')


def append_jsonl(file_path: str, data: Dict[str, Any]) -> None:
    """
    向JSONL文件追加数据
    
    Args:
        file_path: JSONL文件路径
        data: 要追加的数据
    """
    with open(file_path, 'a') as f:
        f.write(json.dumps(data) + '\n')
