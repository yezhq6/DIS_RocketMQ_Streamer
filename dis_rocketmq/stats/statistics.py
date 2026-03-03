#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统计功能模块，用于处理统计信息的更新、计算和打印
"""

from typing import Dict, Any, Optional


# 统计信息更新函数
def update_statistics(statistics: Dict[str, int], node_id: str) -> None:
    """
    更新统计信息：为指定节点的消息计数加1
    
    Args:
        statistics: 统计信息字典，格式为 {node_id: count}
        node_id: 节点ID
    """
    if node_id not in statistics:
        statistics[node_id] = 0
    statistics[node_id] += 1


# 打印统计信息函数
def print_statistics(statistics: Dict[str, int], title: str = "统计信息", operation: str = "发送") -> None:
    """
    打印统计信息
    
    Args:
        statistics: 统计信息字典，格式为 {node_id: count}
        title: 统计信息标题
        operation: 操作类型（如"发送"、"接收"等）
    """
    if not statistics:
        print(f"\n[{title}] 没有统计数据")
        return
    
    # 计算统计数据
    total = sum(statistics.values())
    node_count = len(statistics)
    avg_per_node = total / node_count if node_count > 0 else 0
    
    # 打印统计信息
    print(f"\n[{title}] {operation}统计:")
    print(f"  节点总数: {node_count} 个")
    print(f"  消息总数: {total} 条")
    print(f"  平均每节点: {avg_per_node:.2f} 条")
    # print(f"  {operation}消息节点ID列表前50个: {list(statistics.keys())[:50]}")


# 计算统计信息函数
def calculate_statistics(statistics: Dict[str, int]) -> Dict[str, Any]:
    """
    计算统计信息并返回结果字典
    
    Args:
        statistics: 统计信息字典，格式为 {node_id: count}
        
    Returns:
        包含统计结果的字典
    """
    total = sum(statistics.values())
    node_count = len(statistics)
    avg_per_node = total / node_count if node_count > 0 else 0
    
    return {
        "total": total,
        "node_count": node_count,
        "avg_per_node": avg_per_node,
        "nodes": list(statistics.keys())
    }


# 打印任务总结
def print_task_summary(start_time: float, end_time: float, success_count: int, fail_count: int) -> None:
    """
    打印任务总结
    
    Args:
        start_time: 开始时间（时间戳）
        end_time: 结束时间（时间戳）
        success_count: 成功数量
        fail_count: 失败数量
    """
    total_time = end_time - start_time
    total_count = success_count + fail_count
    success_rate = success_count / total_count * 100 if total_count > 0 else 0
    avg_speed = total_count / total_time if total_time > 0 else 0
    
    print(f"\n=== 任务总结 ===")
    print(f"开始时间: {start_time:.2f}")
    print(f"结束时间: {end_time:.2f}")
    print(f"总耗时: {total_time:.2f} 秒")
    print(f"总数量: {total_count} 条")
    print(f"成功: {success_count} 条")
    print(f"失败: {fail_count} 条")
    print(f"成功率: {success_rate:.2f}%")
    print(f"平均速度: {avg_speed:.2f} 条/秒")
    print(f"================")
