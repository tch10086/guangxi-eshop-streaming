#!/bin/bash
# 兼容旧入口：统一转发到根目录 start.sh（一键启动全部）
HERE="$(cd "$(dirname "$0")" && pwd)"
exec bash "$HERE/../start.sh" "$@"
