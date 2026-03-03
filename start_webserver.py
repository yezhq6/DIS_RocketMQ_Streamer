# 启动Web服务器脚本
import uvicorn

if __name__ == "__main__":
    # 禁用reload模式，使用单进程运行，确保全局变量共享
    uvicorn.run(
        "dis_rocketmq.web.app:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1
    )
