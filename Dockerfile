# 使用Python 3.11 Alpine作为基础镜像，体积更小
FROM swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/python:3.11-alpine3.21

# 设置工作目录
WORKDIR /app

# 设置环境变量
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 安装系统依赖（su-exec 用于在入口脚本中以非 root 用户启动应用）
RUN apk add --no-cache su-exec

# 复制requirements文件并安装Python依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -c "import fastapi, httpx, aioimaplib, pydantic, requests"

# 复制应用代码
COPY main.py .
COPY static/ ./static/
COPY docker-entrypoint.sh .

# 设置启动脚本权限
RUN chmod +x docker-entrypoint.sh

# 创建非 root 运行用户，并创建数据目录（仅属主可访问，保护明文令牌/密码哈希）
RUN addgroup -S appuser && adduser -S appuser -G appuser \
    && mkdir -p /app/data \
    && chown -R appuser:appuser /app /app/data \
    && chmod 700 /app/data

# 暴露端口
EXPOSE 8000

# 健康检查
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/api')" || exit 1

# 启动命令
CMD ["./docker-entrypoint.sh"] 
