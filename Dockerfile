FROM registry-cn-hangzhou.ack.aliyuncs.com/dev/python:3.12-alpine3.21

WORKDIR /app
COPY . /app
RUN pip install /app --no-cache-dir -i https://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com
ENTRYPOINT ["patio"]
