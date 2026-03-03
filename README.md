# DIS RocketMQ 数据处理系统

DIS RocketMQ 数据处理系统是一个用于处理 DIS (Distributed Interactive Simulation) 数据的综合平台，主要功能包括 DIS 数据接收、解析、存储、转发到 RocketMQ，以及提供模拟数据生成和 Web 监控功能。

## 功能特性

### 核心功能
- **DIS 到 RocketMQ 桥接**：实时接收 DIS 数据包并发送到 RocketMQ
- **DIS 数据记录**：记录接收到的 DIS 数据
- **模拟 MQ 生产者**：从 JSONL 文件读取数据并发送到 RocketMQ
- **严格时间控制回放**：基于时间戳的精确数据回放，支持速度调节
- **Web 监控与控制**：提供实时监控和控制界面

### 技术特点
- **模块化设计**：功能组件化，便于扩展和维护
- **配置集中管理**：所有配置参数集中管理，方便统一修改
- **命令行参数支持**：支持通过命令行参数动态调整配置
- **实时统计信息**：提供详细的发送统计和性能指标
- **信号处理**：支持暂停、恢复和停止控制
- **WebSocket 实时通信**：Web 界面与后端实时通信

## 项目结构

```
DIS_ROCKETMQ_codes/
├── dis_rocketmq/          # 核心功能模块
│   ├── config/            # 配置相关功能
│   ├── dis/               # DIS 相关功能
│   │   ├── sender.py      # DIS 数据发送功能
│   │   ├── receiver.py    # DIS 数据接收功能
│   │   └── pdu_parser.py  # DIS PDU 解析功能
│   ├── file/              # 文件处理功能
│   │   ├── jsonl.py       # JSONL 文件处理
│   │   └── entities.py    # 实体处理
│   ├── rocketmq/          # RocketMQ 相关功能
│   │   └── sender.py      # RocketMQ 发送功能
│   ├── controller/        # 控制器功能
│   │   ├── mission_controller.py  # 任务控制器
│   │   └── mission_producer.py    # 模拟 MQ 生产者（Web 服务专属后端）
│   ├── stats/             # 统计相关功能
│   │   └── statistics.py  # 统计功能实现
│   ├── web/               # Web 服务功能
│   │   └── app.py         # Web 应用实现
├── demo/                  # 演示文件
│   ├── dis_receiver_demo.py   # DIS 数据接收演示
│   └── dis_sender_demo.py     # DIS 数据发送演示
├── opendis/               # DIS 协议库
├── static/                # 静态资源文件
│   └── index.html         # Web 界面
├── dis_rocketmq_producer.py  # DIS 到 RocketMQ 桥接服务
├── simulate_mq_producer_bytime.py  # 严格时间控制版模拟 MQ 生产者
├── dis_recorder.py        # DIS 数据记录器
├── start_webserver.py     # Web 服务器启动脚本
├── requirements.txt       # 项目依赖
├── LICENSE                # 许可证文件
└── README.md             # 项目说明文档
```

**注意**：项目中包含一些 Docker 相关文件（如 Dockerfile、docker-compose.yml 等），这些文件用于容器化部署，本文档中不做详细说明。

## 安装与部署

### 环境要求
- Python 3.8+ 
- RocketMQ 服务
- librocketmq (RocketMQ C++ 客户端)

### 安装步骤

#### 克隆项目到本地

```bash
git clone <repository-url>
cd DIS_RocketMQ_Streamer
```

#### 安装 RocketMQ C++ 客户端 (librocketmq)

##### Debian/Ubuntu 系统

```bash
wget https://github.com/apache/rocketmq-client-cpp/releases/download/2.0.0/rocketmq-client-cpp-2.0.0.amd64.deb
sudo dpkg -i rocketmq-client-cpp-2.0.0.amd64.deb
```

##### CentOS/RHEL 系统

```bash
wget https://github.com/apache/rocketmq-client-cpp/releases/download/2.0.0/rocketmq-client-cpp-2.0.0-centos7.x86_64.rpm
sudo rpm -ivh rocketmq-client-cpp-2.0.0-centos7.x86_64.rpm
```

#### 创建并激活虚拟环境

```bash
python -m venv venv
# Windows
venv\Scripts\activate
# Linux/macOS
source venv/bin/activate
```

#### 安装 Python 依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 1. DIS 到 RocketMQ 桥接服务

启动 DIS 到 RocketMQ 的桥接服务：

```bash
python dis_rocketmq_producer.py
```

查看帮助信息：

```bash
python dis_rocketmq_producer.py -h
```

### 2. 严格时间控制版模拟 MQ 生产者

基于时间戳的精确数据回放：

```bash
python simulate_mq_producer_bytime.py
```

查看帮助信息：

```bash
python simulate_mq_producer_bytime.py -h
```

### 3. DIS 数据记录器

记录接收到的 DIS 数据：

```bash
python dis_recorder.py
```

查看帮助信息：

```bash
python dis_recorder.py -h
```

### 4. Web 监控与控制

启动 Web 服务器：

```bash
python start_webserver.py
```

访问 Web 界面：
```
http://localhost:8000/static/index.html
```

### 5. DIS 数据发送演示

发送 DIS 数据：

```bash
python demo/dis_sender_demo.py
```

### 6. DIS 数据接收演示

接收 DIS 数据：

```bash
python demo/dis_receiver_demo.py
```



## 配置管理

所有配置参数都集中在各脚本的 `DEFAULT_CONFIG` 字典中，同时支持通过命令行参数动态调整。

### 核心配置参数

| 参数名 | 描述 | 默认值 |
|-------|------|-------|
| rocketmq_namesrv | RocketMQ NameServer 地址 | 10.125.28.156:9876 |
| rocketmq_topic | RocketMQ 主题 | BattlefieldDeductionSimulation |
| udp_ip | UDP 监听 IP 地址 | 10.10.20.54 |
| udp_port | UDP 监听端口 | 60241 |
| jsonl_file | JSONL 文件路径 | ./dis_recorders/DIS_all_hl.jsonl |
| speed | 回放速度倍率 | 1.0 |


## Web 界面功能

Web 界面提供以下功能：
- 实时监控消息发送状态
- 查看发送统计信息
- 控制任务的开始、暂停、恢复和停止
- 查看实时日志

## 信号控制

mission_producer.py 支持通过信号控制：
- `SIGTERM`/`SIGINT`：停止发送
- `SIGUSR1`：暂停发送
- `SIGUSR2`：恢复发送

## 日志管理

日志文件存储在 `logs/` 目录下，Web 界面提供实时日志查看功能。

## 开发与扩展

### 模块化设计

项目采用模块化设计，便于扩展和维护：
- `dis_rocketmq/config/`：配置相关功能
- `dis_rocketmq/dis/`：DIS 相关功能，包括发送、接收和PDU解析
- `dis_rocketmq/file/`：文件处理功能，包括JSONL文件处理和实体管理
- `dis_rocketmq/rocketmq/`：RocketMQ 相关功能，主要是消息发送
- `dis_rocketmq/controller/`：控制器功能，包括任务控制
- `dis_rocketmq/stats/`：统计相关功能，提供发送统计和性能指标
- `dis_rocketmq/web/`：Web 服务功能，提供监控和控制界面

### 添加新功能

1. 在对应的模块目录下创建新的 Python 文件
2. 实现功能逻辑
3. 在 `__init__.py` 中导出新功能
4. 更新相关脚本或配置

## 测试

### 功能测试

1. 启动 RocketMQ 服务
2. 运行相应的脚本
3. 检查 RocketMQ 中是否收到消息
4. 检查 Web 界面是否正常显示

## 常见问题

### 1. 无法连接到 RocketMQ
- 检查 RocketMQ 服务是否正常运行
- 检查 NameServer 地址是否正确
- 检查网络连接是否正常

### 2. 无法接收 DIS 数据包
- 检查 UDP 监听地址和端口是否正确
- 检查防火墙设置
- 检查发送端是否正确配置

### 3. Web 界面无法访问
- 检查 Web 服务器是否正常运行
- 检查端口是否被占用
- 检查防火墙设置

## 贡献指南

欢迎提交 Issue 和 Pull Request！

### 提交 Pull Request 前请确保：
1. 代码符合项目风格
2. 添加了必要的测试
3. 更新了相关文档
4. 通过了所有测试

## 许可证

[MIT License](LICENSE)

## 联系方式

如有问题或建议，请通过以下方式联系：
- GitHub Issues: [项目 Issues 页面](https://github.com/yourusername/DIS_RocketMQ_Streamer/issues)

## 更新日志

### v1.0.0 (2026-01-21)
- 初始版本发布
- 实现核心功能
- 提供 Web 监控界面
- 支持命令行参数
- 模块化设计

---

感谢使用 DIS RocketMQ Streamer！