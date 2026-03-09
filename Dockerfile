FROM python:3.10-slim

WORKDIR /app

# 安装必要的依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制代码
COPY . .

# 暴露端口（Render 会在这个环境注入 PORT 变量）
EXPOSE 18421

# 使用 __main__.py 启动应用
CMD ["python", "run.py"]
