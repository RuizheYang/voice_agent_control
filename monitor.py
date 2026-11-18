#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from datetime import datetime

import requests
import subprocess
import time
import logging
import argparse  # 新增：用于解析命令行参数
from typing import Dict

# -------------------------- 配置参数（基础配置，环境参数通过命令行传入） --------------------------
HEARTBEAT_URL = "http://localhost:8001/voice-agent-hub/monitor"  # 心跳检测接口URL
CHECK_INTERVAL = 60  # 检测间隔时间（秒）
REQUEST_TIMEOUT = 10  # 接口请求超时时间（秒）
RESTART_SCRIPT_PATH = "./server.sh"  # 重启脚本路径（建议使用绝对路径）
LOG_FILE = "heartbeat_monitor.log"  # 日志文件路径
REQUEST_HEADERS = {
    "Content-Type": "application/json",
}
# --------------------------------------------------------------------------------

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("heartbeat_monitor")


def check_heartbeat() -> bool:
    """检测接口心跳状态（逻辑不变）"""
    try:
        response = requests.get(
            url=HEARTBEAT_URL,
            headers=REQUEST_HEADERS,
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code != 200:
            logger.warning(f"接口返回HTTP状态码异常：{response.status_code}")
            return False

        try:
            result: Dict = response.json()
        except ValueError:
            logger.warning("接口返回非JSON格式数据，服务异常")
            return False

        if "code" not in result or result["code"] != 0:
            logger.warning(f"接口返回code异常：{result.get('code')}（预期0），服务异常")
            return False

        logger.info("接口心跳检测正常")
        return True

    except requests.exceptions.Timeout:
        logger.error(f"接口请求超时（{REQUEST_TIMEOUT}秒），服务可能无响应")
        return False
    except requests.exceptions.ConnectionError:
        logger.error("接口连接失败（可能服务已宕机），服务异常")
        return False
    except Exception as e:
        logger.error(f"心跳检测发生未知错误：{str(e)}，服务异常")
        return False

def send_msg_to_wechat(log_json: dict):
    WEBHOOK_URL = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=aa462180-a422-4100-bfc6-1a2909ab7b38"

    # 将JSON结构转换为易读的文本（企业微信text类型需要字符串内容）
    content = (
        f"【加微Agent异常】\n"
        f"报警时间: {log_json['asctime']}\n"
        f"错误描述: {log_json['message']}"
    )

    # 企微消息payload（text类型）
    payload = {
        "msgtype": "text",
        "text": {"content": content}
    }

    try:
        import requests
        response = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"企微机器人发送失败: {str(e)}")

def restart_service(env: str) -> bool:
    """执行重启脚本（根据传入的环境参数生成-e xxx）"""
    try:
        # 动态生成带环境参数的命令：./server.sh -e {env}
        full_command = f"{RESTART_SCRIPT_PATH} -e {env}"
        logger.info(f"开始执行重启命令：{full_command}")

        result = subprocess.run(
            args=full_command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        logger.info(f"重启命令执行成功，输出：{result.stdout.strip()}")
        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"重启命令执行失败（返回码：{e.returncode}），错误输出：{e.stderr.strip()}")
        return False
    except FileNotFoundError:
        logger.error(f"重启脚本不存在：{RESTART_SCRIPT_PATH}（请检查路径是否正确）")
        return False
    except Exception as e:
        logger.error(f"执行重启脚本发生未知错误：{str(e)}")
        return False


def main():
    # 新增：解析命令行参数（--env 指定环境，默认prod）
    parser = argparse.ArgumentParser(description="心跳检测监控程序")
    parser.add_argument("--env", type=str, default="prod", choices=["prod", "test"],
                        help="指定环境（prod=生产环境，test=测试环境，默认prod）")
    args = parser.parse_args()  # 解析参数

    # 日志显示当前环境
    logger.info("====== 心跳检测监控程序启动 ======")
    logger.info(f"检测接口：{HEARTBEAT_URL}")
    logger.info(f"检测间隔：{CHECK_INTERVAL}秒")
    logger.info(f"当前环境：{args.env}，重启命令：{RESTART_SCRIPT_PATH} -e {args.env}")

    try:
        while True:
            is_healthy = check_heartbeat()
            if not is_healthy:
                logger.warning("服务异常，准备执行重启...")
                # 传入环境参数执行重启
                restart_success = restart_service(env=args.env)
                send_msg_to_wechat({"asctime": datetime.now(), "message": "服务异常，准备执行重启..."})
                if restart_success:
                    logger.info("服务重启完成，等待下一次检测...")
                    send_msg_to_wechat({"asctime": datetime.now(), "message": "服务重启完成，等待下一次检测..."})
                else:
                    logger.error("服务重启失败，请手动检查！")
                    send_msg_to_wechat({"asctime": datetime.now(), "message": "服务重启失败，请手动检查！"})
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        logger.info("用户中断程序，退出监控")
    except Exception as e:
        logger.critical(f"监控程序发生致命错误：{str(e)}，程序退出", exc_info=True)


if __name__ == "__main__":
    main()