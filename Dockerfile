# Patio engine-runtime sidecar image.
#
# Build:
#   docker build -t patio-runtime:dev .
# The default entrypoint runs `patio`; pass app arguments after the image name,
# for example:
#   docker run patio-runtime:dev --events-config /etc/patio/events.yaml \
#     --instance-info '{"topo_type":"sglang","data":{"port":"30000"}}'
FROM registry-cn-hangzhou.ack.aliyuncs.com/dev/python:3.12-alpine3.21

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install the patio package only; everything else stays out of the image via
# .dockerignore.
COPY pyproject.toml README.md LICENSE ./
COPY src/ src/
RUN pip install --no-cache-dir \
      -i https://mirrors.aliyun.com/pypi/simple/ \
      --trusted-host mirrors.aliyun.com \
      .

# Run as the unprivileged patio user.
RUN adduser -D -u 1000 patio
USER patio

EXPOSE 9091

ENTRYPOINT ["patio"]
