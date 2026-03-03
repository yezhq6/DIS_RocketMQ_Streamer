from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
import subprocess
import os
import asyncio
import datetime
import time
import signal
from typing import List, Dict, Any
from contextlib import asynccontextmanager

app = FastAPI(title="任务触发服务")

# WebSocket连接管理器
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.message_count = 0
        self.start_time = None
        self.first_data_timestamp = None  # 第一条数据的时间戳
        self.simulation_relative_time = 0.0  # 仿真相对时间：基于JSONL文件中时间戳的差值
        self.total_nodes = 0  # 总节点数量
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"[WebSocket] 新连接已建立，当前连接数: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        print(f"[WebSocket] 连接已关闭，当前连接数: {len(self.active_connections)}")
    
    async def broadcast(self, message: Dict[str, Any]):
        """向所有连接的客户端广播消息"""
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                print(f"[WebSocket] 广播消息失败: {e}")
                disconnected.append(connection)
        
        # 清理断开的连接
        for connection in disconnected:
            self.disconnect(connection)
    
    def update_message_count(self, count: int):
        """更新消息计数"""
        self.message_count = count
    
    def get_message_count(self) -> int:
        return self.message_count
    
    def set_start_time(self, start_time: float):
        self.start_time = start_time
    
    def set_first_data_timestamp(self, timestamp: float):
        """设置第一条数据的时间戳"""
        if self.first_data_timestamp is None:
            self.first_data_timestamp = timestamp
    
    def set_simulation_relative_time(self, relative_time: float):
        """更新仿真相对时间：基于JSONL文件中时间戳的差值"""
        self.simulation_relative_time = relative_time
    
    def get_relative_time(self) -> float:
        """获取相对时间：返回基于JSONL文件中时间戳的仿真相对时间"""
        return self.simulation_relative_time
    
    def set_total_nodes(self, nodes: int):
        """设置总节点数量"""
        self.total_nodes = nodes
    
    def get_total_nodes(self) -> int:
        """获取总节点数量"""
        return self.total_nodes

# 创建连接管理器实例
manager = ConnectionManager()

# 全局变量，用于存储当前运行脚本的进程ID、日志文件路径和状态
current_process = None
log_file_path = None
current_status = "stopped"  # 初始状态为stopped，可选值：stopped, running, paused

# 挂载静态网页
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def home():
    return {"message": "访问 /static/index.html 使用控制面板"}

# WebSocket端点
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # 声明全局变量
        global current_process, current_status
        
        while True:
            # 接收客户端消息
            data = await websocket.receive_json()
            print(f"[WebSocket] 收到消息: {data}")
            
            # 处理客户端命令
            if "command" in data:
                command = data["command"]
                
                if command == "get_status":
                    # 返回当前状态，使用全局current_status变量
                    await websocket.send_json({
                        "type": "status",
                        "status": current_status,
                        "sent_count": manager.get_message_count(),
                        "relative_time": manager.get_relative_time(),
                        "total_nodes": manager.get_total_nodes()
                    })
                elif command == "ping":
                    # 心跳响应
                    await websocket.send_json({
                        "type": "pong",
                        "timestamp": time.time()
                    })
                elif command == "pause":
                    # 暂停任务
                    print(f"[WebSocket] 收到暂停命令")
                    if current_process and current_process.returncode is None:
                        try:
                            # 发送SIGUSR1信号暂停进程
                            current_process.send_signal(signal.SIGUSR1)
                            print(f"[WebSocket] 已发送SIGUSR1信号到进程 {current_process.pid}")
                            
                            # 更新全局状态为paused
                            current_status = "paused"
                            
                            # 广播状态更新
                            await manager.broadcast({
                                "type": "status",
                                "status": "paused",
                                "sent_count": manager.get_message_count(),
                                "relative_time": manager.get_relative_time(),
                                "total_nodes": manager.get_total_nodes()
                            })
                            
                            await websocket.send_json({
                                "type": "response",
                                "command": "pause",
                                "status": "success",
                                "message": "任务已暂停"
                            })
                        except Exception as e:
                            print(f"[WebSocket] 暂停进程失败: {e}")
                            await websocket.send_json({
                                "type": "response",
                                "command": "pause",
                                "status": "error",
                                "message": f"暂停失败: {str(e)}"
                            })
                    else:
                        await websocket.send_json({
                            "type": "response",
                            "command": "pause",
                            "status": "error",
                            "message": "没有正在运行的任务"
                        })
                elif command == "resume":
                    # 继续任务
                    print(f"[WebSocket] 收到继续命令")
                    if current_process and current_process.returncode is None:
                        try:
                            # 发送SIGUSR2信号继续进程
                            current_process.send_signal(signal.SIGUSR2)
                            print(f"[WebSocket] 已发送SIGUSR2信号到进程 {current_process.pid}")
                            
                            # 更新全局状态为running
                            current_status = "running"
                            
                            # 广播状态更新
                            await manager.broadcast({
                                "type": "status",
                                "status": "running",
                                "sent_count": manager.get_message_count(),
                                "relative_time": manager.get_relative_time(),
                                "total_nodes": manager.get_total_nodes()
                            })
                            
                            await websocket.send_json({
                                "type": "response",
                                "command": "resume",
                                "status": "success",
                                "message": "任务已继续"
                            })
                        except Exception as e:
                            print(f"[WebSocket] 继续进程失败: {e}")
                            await websocket.send_json({
                                "type": "response",
                                "command": "resume",
                                "status": "error",
                                "message": f"继续失败: {str(e)}"
                            })
                    else:
                        await websocket.send_json({
                            "type": "response",
                            "command": "resume",
                            "status": "error",
                            "message": "没有正在运行的任务"
                        })
                elif command == "stop":
                    # 停止任务
                    print(f"[WebSocket] 收到停止命令")
                    if current_process and current_process.returncode is None:
                        # 获取进程ID以便调试
                        pid = current_process.pid
                        print(f"[WebSocket] 正在终止进程 {pid}")
                        
                        # 先尝试发送SIGTERM信号
                        current_process.send_signal(signal.SIGTERM)
                        
                        # 等待进程终止（最多等待5秒）
                        try:
                            await asyncio.wait_for(current_process.wait(), timeout=5.0)
                            print(f"[WebSocket] 进程 {pid} 已终止")
                        except asyncio.TimeoutError:
                            print(f"[WebSocket] 进程 {pid} 终止超时，尝试强制杀死")
                            # 尝试发送SIGKILL信号
                            current_process.send_signal(signal.SIGKILL)
                            try:
                                await asyncio.wait_for(current_process.wait(), timeout=2.0)
                                print(f"[WebSocket] 进程 {pid} 已被强制杀死")
                            except asyncio.TimeoutError:
                                print(f"[WebSocket] 进程 {pid} 无法被杀死")
                        
                        # 更新全局状态为stopped
                        current_status = "stopped"
                        
                        # 向发送命令的客户端发送响应
                        await websocket.send_json({
                            "type": "response",
                            "command": "stop",
                            "status": "success",
                            "message": "任务已停止"
                        })
                        
                        # 立即广播状态更新
                        await manager.broadcast({
                            "type": "status",
                            "status": "stopped",
                            "sent_count": manager.get_message_count(),
                            "relative_time": manager.get_relative_time(),
                            "total_nodes": manager.get_total_nodes()
                        })
                        
                        # 广播任务完成总结信息
                        await manager.broadcast({
                            "type": "real_time_data",
                            "timestamp": time.time(),
                            "log_line": f"本次任务结束 | 总计发送消息数: {manager.get_message_count()} | 仿真时长: {manager.get_relative_time():.2f}秒",
                            "message": "任务总结"
                        })
                    else:
                        await websocket.send_json({
                            "type": "response",
                            "command": "stop",
                            "status": "error",
                            "message": "没有正在运行的任务"
                        })
            
            # 定期发送实时状态更新
            await websocket.send_json({
                "type": "status",
                "status": current_status,
                "sent_count": manager.get_message_count(),
                "relative_time": manager.get_relative_time(),
                "total_nodes": manager.get_total_nodes()
            })
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        print(f"[WebSocket] 连接错误: {e}")
        manager.disconnect(websocket)

# 异步任务函数：处理进程输出，直接推送到前端并提取关键信息
async def log_process_output(process, log_path, script_name):
    # 声明全局变量
    global current_status
    
    # 初始化管理器的开始时间
    manager.set_start_time(time.time())
    
    # 不再广播脚本开始运行的通知
    
    # 异步任务：读取并处理stdout，直接推送到前端，同时解析关键信息
    async def process_stdout():
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            line_str = line.decode('utf-8', errors='replace').strip()
            
            # 跳过空行
            if not line_str:
                continue
            
            # 过滤掉不需要的日志行
            if "=== 脚本开始运行:" not in line_str and \
               "=== 脚本运行结束:" not in line_str and \
               "发送失败: " not in line_str and \
               "======================================================================" not in line_str:
                # 直接将日志推送到前端
                await manager.broadcast({
                    "type": "real_time_data",
                    "timestamp": time.time(),
                    "log_line": line_str,
                    "message": "日志更新"
                })
            
            # 解析关键信息：从进度行中提取消息计数和相对仿真时间
            if " | 成功: " in line_str and " | 失败: " in line_str and "相对仿真时间: " in line_str:
                try:
                    # 提取总消息数：格式为 "100 | 成功: 100 | 失败: 0 | 速度: 100.0条/秒 | 相对仿真时间: 1.00秒"
                    progress_end = line_str.find(" | ")
                    if progress_end > 0:
                        total_count_str = line_str[:progress_end].strip()
                        total_count = int(total_count_str.replace(",", ""))
                        # 更新管理器中的消息计数
                        manager.update_message_count(total_count)
                    
                    # 提取相对仿真时间
                    if "相对仿真时间: " in line_str:
                        rel_time_start = line_str.find("相对仿真时间: ") + len("相对仿真时间: ")
                        rel_time_end = line_str.find("秒", rel_time_start)
                        if rel_time_end > rel_time_start:
                            rel_time_str = line_str[rel_time_start:rel_time_end].strip()
                            relative_time = float(rel_time_str)
                            # 更新管理器中的相对仿真时间
                            manager.set_simulation_relative_time(relative_time)
                    
                    # 广播实时状态
                    await manager.broadcast({
                        "type": "status",
                        "status": "running",
                        "sent_count": total_count,
                        "relative_time": manager.get_relative_time(),
                        "total_nodes": manager.get_total_nodes()
                    })
                    
                    # 广播实时数据反馈
                    await manager.broadcast({
                        "type": "real_time_data",
                        "timestamp": time.time(),
                        "sent_count": total_count,
                        "relative_time": manager.get_relative_time(),
                        "message": f"处理进度: {total_count} 条"
                    })
                except Exception as e:
                    print(f"[日志解析] 解析进度行失败: {e}")
            # 解析总节点数量
            elif "总节点数量: " in line_str:
                try:
                    # 提取总节点数量
                    nodes_start = line_str.find("总节点数量: ") + len("总节点数量: ")
                    nodes_str = line_str[nodes_start:].strip()
                    total_nodes = int(nodes_str)
                    # 更新管理器中的总节点数量
                    manager.set_total_nodes(total_nodes)
                    
                    # 广播总节点数量更新
                    await manager.broadcast({
                        "type": "status",
                        "status": "running",
                        "sent_count": manager.get_message_count(),
                        "relative_time": manager.get_relative_time(),
                        "total_nodes": total_nodes
                    })
                except Exception as e:
                    print(f"[日志解析] 解析总节点数量失败: {e}")
    
    # 异步任务：读取并处理stderr，直接推送到前端
    async def process_stderr():
        while True:
            line = await process.stderr.readline()
            if not line:
                break
            line_str = line.decode('utf-8', errors='replace').strip()
            
            # 跳过空行
            if not line_str:
                continue
            
            # 直接将错误日志推送到前端
            await manager.broadcast({
                "type": "real_time_data",
                "timestamp": time.time(),
                "log_line": line_str,
                "message": "错误日志"
            })
    
    # 并行运行日志读取任务，同时等待进程完成
    await asyncio.gather(
        process_stdout(),
        process_stderr(),
        process.wait(),
        return_exceptions=True
    )
    
    # 获取进程退出码
    exit_code = process.returncode
    
    # 更新全局状态为stopped
    current_status = "stopped"
    
    # 不再广播脚本运行结束的通知
    
    # 广播任务完成状态
    await manager.broadcast({
        "type": "status",
        "status": "stopped",
        "sent_count": manager.get_message_count(),
        "relative_time": manager.get_relative_time(),
        "total_nodes": manager.get_total_nodes()
    })
    
    # 不再广播总结信息，已在停止命令处理函数中广播

@app.get("/get-logs")
async def get_logs():
    """
    获取当前运行脚本的日志内容
    注意：日志现在通过WebSocket实时推送，此端点仅返回空结果
    """
    # 检查进程是否仍在运行
    global current_process
    is_running = current_process and current_process.returncode is None
    
    return {
        "status": "success",
        "content": "日志现在通过WebSocket实时推送，请查看前端日志信息面板",
        "file_size": 0,
        "is_running": is_running,
        "log_file": None
    }


@app.get("/get-status")
async def get_status():
    """
    获取当前仿真状态
    """
    global current_process, current_status
    
    # 检查当前进程状态
    if current_process and current_process.returncode is None:
        # 进程正在运行
        status = current_status
        if status == "running":
            message = "任务运行中"
        elif status == "paused":
            message = "任务暂停"
        else:
            message = f"任务{status}"
    else:
        # 进程已停止
        status = "stopped"
        message = "任务已停止"
    
    # 返回状态信息，不包含pid字段
    return {
        "status": status,
        "message": message,
        "sent_count": manager.get_message_count(),
        "relative_time": manager.get_relative_time()
    }

@app.post("/run-task")
async def run_task(script: str = "simulate_mq_producer.py"):
    """
    运行指定的脚本
    :param script: 要运行的脚本名称，默认为 simulate_mq_producer.py
    """
    # 检查是否已有进程在运行（包括暂停状态）
    global current_process, current_status
    if current_process and current_process.returncode is None:
        return {
            "status": "error",
            "message": "已有任务在运行中（或暂停），请先停止当前任务"
        }
    
    # 支持的脚本列表
    supported_scripts = {
        "simulate": "dis_rocketmq/controller/mission_producer.py",
        "dis_producer": "dis_rocketmq_producer.py",
        "test": "test_infinite_send.py",
        "mission_producer.py": "dis_rocketmq/controller/mission_producer.py",
        "dis_rocketmq_producer.py": "dis_rocketmq_producer.py",
        "test_infinite_send.py": "test_infinite_send.py"
    }
    
    # 获取实际脚本名称
    script_name = supported_scripts.get(script, script)
    
    if not os.path.exists(script_name):
        return {"error": f"脚本 {script_name} 不存在"}
    
    try:
        # 生成日志文件路径
        global log_file_path
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 创建logs文件夹（如果不存在）
        logs_dir = "logs"
        os.makedirs(logs_dir, exist_ok=True)
        
        # 日志文件存储到logs文件夹
        log_file_path = os.path.join(logs_dir, f"logs_{timestamp}.txt")
        
        # 直接使用python解释器，不通过shell创建进程，更可靠
        # 添加-u选项禁用Python输出缓冲，确保进度行实时输出
        python_cmd = ["python", "-u"]
        
        # 为不同脚本添加参数
        if script_name == "dis_rocketmq_producer.py":
            # 使用默认参数运行dis_rocketmq_producer.py
            cmd_list = python_cmd + [script_name]
        else:
            # 使用默认参数运行mission_producer.py，添加必要的参数
            cmd_list = python_cmd + [script_name, "-b", "100"]
        
        print(f"[run-task] 执行命令: {' '.join(cmd_list)}")
        
        # 直接创建子进程，不使用shell，确保信号能正确传递
        process = await asyncio.create_subprocess_exec(
            *cmd_list,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        print(f"[run-task] 子进程已创建，PID: {process.pid}")
        
        # 保存当前进程和状态（已在函数开头声明为全局变量）
        current_process = process
        current_status = "running"
        
        # 重置消息计数
        manager.update_message_count(0)
        manager.set_start_time(time.time())
        
        # 广播状态更新
        await manager.broadcast({
            "type": "status",
            "status": "running",
            "sent_count": manager.get_message_count(),
            "relative_time": manager.get_relative_time(),
            "total_nodes": manager.get_total_nodes()
        })
        
        # 在后台异步记录日志，不阻塞返回响应
        asyncio.create_task(log_process_output(process, log_file_path, script_name))
        
        # 立即返回任务启动成功的消息
        return {
            "status": "started",
            "message": f"脚本 {script_name} 已成功启动，正在后台运行...",
            "log_file": log_file_path,
            "script": script_name,
            "sent_count": manager.get_message_count(),
            "relative_time": manager.get_relative_time()
        }
    except Exception as e:
        return {"error": f"启动脚本时出错: {str(e)}"}


@app.post("/pause-task")
async def pause_task():
    """
    暂停当前运行的任务
    """
    global current_process, current_status
    
    if not current_process or current_process.returncode is not None:
        return {
            "status": "error",
            "message": "没有正在运行的任务"
        }
    
    try:
        # 发送SIGUSR1信号暂停进程
        current_process.send_signal(signal.SIGUSR1)
        print(f"[pause-task] 已发送SIGUSR1信号到进程 {current_process.pid}")
        
        # 更新全局状态为paused
        current_status = "paused"
        
        # 广播任务已暂停日志信息
        await manager.broadcast({
            "type": "real_time_data",
            "timestamp": time.time(),
            "log_line": "任务已暂停",
            "message": "任务状态更新"
        })
        
        # 广播状态更新
        await manager.broadcast({
            "type": "status",
            "status": "paused",
            "sent_count": manager.get_message_count(),
            "relative_time": manager.get_relative_time(),
            "total_nodes": manager.get_total_nodes()
        })
        
        return {
            "status": "paused",
            "message": "任务已暂停",
            "sent_count": manager.get_message_count(),
            "relative_time": manager.get_relative_time()
        }
    except Exception as e:
        print(f"[pause-task] 暂停进程失败: {e}")
        return {
            "status": "error",
            "message": f"暂停失败: {str(e)}"
        }


@app.post("/resume-task")
async def resume_task():
    """
    恢复当前暂停的任务
    """
    global current_process, current_status
    
    if not current_process or current_process.returncode is not None:
        return {
            "status": "error",
            "message": "没有正在运行的任务"
        }
    
    try:
        # 发送SIGUSR2信号恢复进程
        current_process.send_signal(signal.SIGUSR2)
        print(f"[resume-task] 已发送SIGUSR2信号到进程 {current_process.pid}")
        
        # 更新全局状态为running
        current_status = "running"
        
        # 广播任务已继续日志信息
        await manager.broadcast({
            "type": "real_time_data",
            "timestamp": time.time(),
            "log_line": "任务已继续",
            "message": "任务状态更新"
        })
        
        # 广播状态更新
        await manager.broadcast({
            "type": "status",
            "status": "running",
            "sent_count": manager.get_message_count(),
            "relative_time": manager.get_relative_time(),
            "total_nodes": manager.get_total_nodes()
        })
        
        return {
            "status": "running",
            "message": "任务已恢复",
            "sent_count": manager.get_message_count(),
            "relative_time": manager.get_relative_time()
        }
    except Exception as e:
        print(f"[resume-task] 恢复进程失败: {e}")
        return {
            "status": "error",
            "message": f"恢复失败: {str(e)}"
        }


@app.post("/stop-task")
async def stop_task():
    """
    停止当前运行的任务
    """
    global current_process, current_status
    
    if not current_process or current_process.returncode is not None:
        return {
            "status": "error",
            "message": "没有正在运行的任务"
        }
    
    try:
        # 获取进程ID以便调试
        pid = current_process.pid
        print(f"[stop-task] 正在终止进程 {pid}")
        
        # 先尝试发送SIGTERM信号
        current_process.send_signal(signal.SIGTERM)
        
        # 等待进程终止（最多等待5秒）
        await asyncio.wait_for(current_process.wait(), timeout=5.0)
        print(f"[stop-task] 进程 {pid} 已终止")
        
        # 更新全局状态为stopped
        current_status = "stopped"
        
        # 广播状态更新
        await manager.broadcast({
            "type": "status",
            "status": "stopped",
            "sent_count": manager.get_message_count(),
            "relative_time": manager.get_relative_time(),
            "total_nodes": manager.get_total_nodes()
        })
        
        # 广播任务完成总结信息
        await manager.broadcast({
            "type": "real_time_data",
            "timestamp": time.time(),
            "log_line": f"本次任务结束 | 总计发送消息数: {manager.get_message_count()} | 仿真时长: {manager.get_relative_time():.2f}秒",
            "message": "任务总结"
        })
        
        return {
            "status": "stopped",
            "message": "任务已停止",
            "sent_count": manager.get_message_count(),
            "relative_time": manager.get_relative_time()
        }
    except asyncio.TimeoutError:
        print(f"[stop-task] 进程 {pid} 终止超时，尝试强制杀死")
        # 尝试发送SIGKILL信号
        current_process.send_signal(signal.SIGKILL)
        try:
            await asyncio.wait_for(current_process.wait(), timeout=2.0)
            print(f"[stop-task] 进程 {pid} 已被强制杀死")
            
            # 更新全局状态为stopped
            current_status = "stopped"
            
            # 广播状态更新
            await manager.broadcast({
                "type": "status",
                "status": "stopped",
                "sent_count": manager.get_message_count(),
                "relative_time": manager.get_relative_time(),
                "total_nodes": manager.get_total_nodes()
            })
            
            # 广播任务完成总结信息
            await manager.broadcast({
                "type": "real_time_data",
                "timestamp": time.time(),
                "log_line": f"本次任务结束 | 总计发送消息数: {manager.get_message_count()} | 仿真时长: {manager.get_relative_time():.2f}秒",
                "message": "任务总结"
            })
            
            return {
                "status": "stopped",
                "message": "任务已强制停止",
                "sent_count": manager.get_message_count(),
                "relative_time": manager.get_relative_time()
            }
        except asyncio.TimeoutError:
            print(f"[stop-task] 进程 {pid} 无法被杀死")
            return {
                "status": "error",
                "message": f"进程 {pid} 无法被杀死"
            }
    except Exception as e:
        print(f"[stop-task] 停止进程失败: {e}")
        return {
            "status": "error",
            "message": f"停止失败: {str(e)}"
        }