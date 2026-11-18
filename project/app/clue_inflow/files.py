"""受控文件存储和流式解析；多节点需挂载同一个持久目录。"""
import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import re
import socket
import uuid
import zipfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from xml.etree.ElementTree import ParseError

import aiohttp
from aiohttp.resolver import DefaultResolver

from app.clue_inflow import InflowError

MAX_FILE_BYTES = 200 * 1024 * 1024
MAX_ROW_BYTES = 256 * 1024
logger = logging.getLogger(__name__)


class PublicResolver(DefaultResolver):
    async def resolve(self, host, port=0, family=socket.AF_INET):
        records = await super().resolve(host, port, family)
        if any(not ipaddress.ip_address(record["host"]).is_global for record in records):
            raise OSError("文件地址解析到了非公网地址")
        return records


@dataclass
class FileRow:
    number: int
    value: object = None
    error: str | None = None


class FileStore:
    def __init__(self, root: Path | None = None):
        default = Path(__file__).resolve().parents[2] / "dbfiles" / "clue-imports"
        self.root = (root or Path(os.getenv("CLUE_IMPORT_DIR", str(default)))).resolve()

    def path(self, key: str) -> Path:
        if not re.fullmatch(r"[a-f0-9]{32}\.(bin|json)", key):
            raise InflowError(40006, "文件对象标识不合法")
        return self.root / key

    def _open_new(self, suffix="bin"):
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        key = f"{uuid.uuid4().hex}.{suffix}"
        path = self.path(key)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        return key, os.fdopen(descriptor, "wb")

    def save_bytes(self, content: bytes):
        if len(content) > MAX_FILE_BYTES:
            raise InflowError(40001, "文件超过大小上限", 413)
        key, output = self._open_new()
        with output:
            output.write(content)
            output.flush()
            os.fsync(output.fileno())
        logger.debug("[Clue-Inflow] 【完成】文件内容已保存: object_key=%s, size_bytes=%s", key, len(content))
        return key, hashlib.sha256(content).hexdigest()

    async def save_upload(self, upload):
        key, output = self._open_new()
        digest, size = hashlib.sha256(), 0
        logger.info("[Clue-Inflow] 【开始】保存上传文件: object_key=%s", key)
        try:
            with output:
                while chunk := await upload.read(64 * 1024):
                    size += len(chunk)
                    if size > MAX_FILE_BYTES:
                        raise InflowError(40001, "文件超过200MiB上限", 413)
                    digest.update(chunk)
                    await asyncio.to_thread(output.write, chunk)
                await asyncio.to_thread(output.flush)
                await asyncio.to_thread(os.fsync, output.fileno())
            logger.info("[Clue-Inflow] 【完成】上传文件已保存: object_key=%s, size_bytes=%s", key, size)
            return key, digest.hexdigest()
        except BaseException as error:
            self.remove(key)
            logger.log(logging.INFO if isinstance(error, asyncio.CancelledError) else logging.WARNING,
                       "[Clue-Inflow] 【清理】上传文件保存未完成，临时文件已删除: object_key=%s, size_bytes=%s, error_type=%s",
                       key, size, type(error).__name__)
            raise

    def remove(self, key):
        self.path(key).unlink(missing_ok=True)
        logger.debug("[Clue-Inflow] 【清理】文件对象已删除: object_key=%s", key)

    def write_errors(self, errors):
        key, output = self._open_new("json")
        with output:
            output.write(json.dumps(errors, ensure_ascii=False).encode("utf-8"))
            output.flush()
            os.fsync(output.fileno())
        logger.debug("[Clue-Inflow] 【错误明细】行错误文件已保存，等待任务事务提交引用: object_key=%s, error_count=%s",
                     key, len(errors))
        return key

    def read_errors(self, key):
        logger.debug("[Clue-Inflow] 【查询】读取行错误文件: object_key=%s", key)
        return json.loads(self.path(key).read_text(encoding="utf-8"))

    def validate_url(self, url):
        parts = urlsplit(url)
        allowed = {host.strip().lower() for host in os.getenv("CLUE_IMPORT_ALLOWED_HOSTS", "").split(",") if host.strip()}
        if (parts.scheme != "https" or not parts.hostname or parts.hostname.lower() not in allowed
                or parts.username or parts.password or parts.fragment or parts.port not in (None, 443)):
            raise InflowError(40001, "file_url 必须是已配置白名单内的 HTTPS 对象存储地址")
        try:
            address = ipaddress.ip_address(parts.hostname)
        except ValueError:
            address = None
        if address is not None and not address.is_global:
            raise InflowError(40001, "file_url不能指向非公网地址")
        return url

    async def fetch(self, url, expected_sha256):
        self.validate_url(url)
        key, output = self._open_new()
        digest, size = hashlib.sha256(), 0
        logger.info("[Clue-Inflow] 【开始】下载远程导入文件: object_key=%s", key)
        try:
            connector = aiohttp.TCPConnector(resolver=PublicResolver())
            async with aiohttp.ClientSession(connector=connector, timeout=aiohttp.ClientTimeout(total=120)) as client:
                async with client.get(url, allow_redirects=False) as response:
                    if response.status != 200:
                        raise InflowError(40006, "远程文件读取失败")
                    with output:
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            size += len(chunk)
                            if size > MAX_FILE_BYTES:
                                raise InflowError(40006, "远程文件超过200MiB上限")
                            digest.update(chunk)
                            await asyncio.to_thread(output.write, chunk)
                        await asyncio.to_thread(output.flush)
                        await asyncio.to_thread(os.fsync, output.fileno())
            if digest.hexdigest() != expected_sha256.lower():
                raise InflowError(40002, "远程文件 SHA256 校验失败")
            logger.info("[Clue-Inflow] 【完成】远程文件已下载且哈希校验通过: object_key=%s, size_bytes=%s", key, size)
            return key
        except BaseException as error:
            output.close()
            self.remove(key)
            logger.log(logging.INFO if isinstance(error, asyncio.CancelledError) else logging.WARNING,
                       "[Clue-Inflow] 【清理】远程文件下载未完成，临时文件已删除: object_key=%s, size_bytes=%s, error_type=%s",
                       key, size, type(error).__name__)
            raise

    def iter_rows(self, key, file_type, after=0):
        logger.debug("[Clue-Inflow] 【开始】解析导入文件: object_key=%s, file_type=%s, after_row=%s", key, file_type, after)
        try:
            if file_type == "jsonl":
                yield from self._jsonl_rows(key, after)
            else:
                yield from self._excel_rows(key, after)
        except (UnicodeError, zipfile.BadZipFile, OSError, ValueError, ParseError, KeyError, IndexError) as error:
            logger.warning("[Clue-Inflow] 【失败】导入文件无法解析: object_key=%s, file_type=%s, error_type=%s",
                           key, file_type, type(error).__name__)
            raise InflowError(40006, "文件编码、格式或内容损坏") from error

    def _jsonl_rows(self, key, after):
        with self.path(key).open("r", encoding="utf-8-sig") as source:
            number = 0
            while line := source.readline(MAX_ROW_BYTES + 1):
                number += 1
                oversized = len(line.encode("utf-8")) > MAX_ROW_BYTES
                if oversized and not line.endswith("\n"):
                    while tail := source.readline(MAX_ROW_BYTES + 1):
                        if tail.endswith("\n"):
                            break
                if number <= after or not line.strip():
                    continue
                if oversized:
                    yield FileRow(number, error="单行内容超过256KiB上限")
                    continue
                try:
                    yield FileRow(number, json.loads(line))
                except json.JSONDecodeError:
                    yield FileRow(number, error="该行不是合法JSON对象")

    def _excel_rows(self, key, after):
        from openpyxl import load_workbook
        from openpyxl.utils.exceptions import InvalidFileException
        with self.path(key).open("rb") as source:
            try:
                with zipfile.ZipFile(source) as archive:
                    if sum(info.file_size for info in archive.infolist()) > 512 * 1024 * 1024:
                        raise InflowError(40006, "Excel解压后内容超过512MiB上限")
                source.seek(0)
                workbook = load_workbook(source, read_only=True, data_only=True)
            except InvalidFileException as error:
                raise InflowError(40006, "文件不是有效的xlsx工作簿") from error
            try:
                rows = workbook.worksheets[0].iter_rows(values_only=True)
                header = next(rows, ())
                names = [str(value).strip() if value is not None else "" for value in header]
                required = {"clue_id", "agent_data", "phone_number"}
                nonempty = [name for name in names if name]
                if not required.issubset(names) or len(nonempty) != len(set(nonempty)):
                    raise InflowError(40006, "Excel缺少必需表头或存在重复表头")
                allowed = required | {"external_task_id"}
                if set(nonempty) - allowed:
                    raise InflowError(40006, "Excel包含未定义表头，扩展业务数据请放入agent_data")
                for number, values in enumerate(rows, 2):
                    if number <= after or not any(value is not None for value in values):
                        continue
                    data = {name: value for name, value in zip(names, values) if name}
                    if data.get("external_task_id") is None:
                        data.pop("external_task_id", None)
                    if isinstance(data.get("phone_number"), (int, float)):
                        phone = data["phone_number"]
                        data["phone_number"] = str(int(phone)) if int(phone) == phone else str(phone)
                    try:
                        data["agent_data"] = json.loads(data["agent_data"])
                        yield FileRow(number, data)
                    except (TypeError, json.JSONDecodeError):
                        yield FileRow(number, error="agent_data单元格必须是JSON对象字符串")
            finally:
                workbook.close()


def next_chunk(iterator, size=200):
    rows = []
    for _ in range(size):
        item = next(iterator, None)
        if item is None:
            break
        rows.append(item)
    return rows
