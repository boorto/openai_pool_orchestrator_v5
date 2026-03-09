"""
存储后端抽象层
===============
通过环境变量 STORAGE_BACKEND 切换后端：
  - "file"  (默认) → 本地文件系统，与原有行为完全一致
  - "redis"          → Redis (推荐 Upstash，适配 Render 等无盘容器云)

环境变量：
  STORAGE_BACKEND  = file | redis
  REDIS_URL        = redis://... (仅 redis 后端需要)
"""

import json
import os
import re
import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


# ==========================================
# 抽象基类
# ==========================================

class StorageBackend(ABC):
    """存储后端统一接口"""

    # ---- 配置 ----
    @abstractmethod
    def load_config(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def save_config(self, cfg: Dict[str, Any]) -> None:
        ...

    # ---- 统计 ----
    @abstractmethod
    def load_state(self) -> Dict[str, int]:
        ...

    @abstractmethod
    def save_state(self, success: int, fail: int) -> None:
        ...

    # ---- Token ----
    @abstractmethod
    def save_token(self, filename: str, data: str) -> None:
        """保存 Token，data 为 JSON 字符串"""
        ...

    @abstractmethod
    def load_token(self, filename: str) -> Optional[Dict[str, Any]]:
        """读取 Token 并解析为 dict，不存在返回 None"""
        ...

    @abstractmethod
    def list_tokens(self) -> List[str]:
        """列出所有 Token 文件名，按时间戳降序排列"""
        ...

    @abstractmethod
    def delete_token(self, filename: str) -> bool:
        """删除指定 Token，成功返回 True"""
        ...

    @abstractmethod
    def update_token(self, filename: str, data: Dict[str, Any]) -> bool:
        """更新 Token 内容（用于标记已上传等），成功返回 True"""
        ...

    @abstractmethod
    def token_exists(self, filename: str) -> bool:
        """检查 Token 是否存在"""
        ...

    @abstractmethod
    def load_token_raw(self, filename: str) -> Optional[str]:
        """读取 Token 原始 JSON 字符串"""
        ...


# ==========================================
# 默认配置
# ==========================================

_DEFAULT_CONFIG: Dict[str, Any] = {
    "base_url": "", "bearer_token": "", "account_name": "AutoReg", "auto_sync": "false",
    "cpa_base_url": "", "cpa_token": "", "min_candidates": 800,
    "used_percent_threshold": 95, "auto_maintain": False, "maintain_interval_minutes": 30,
    "upload_mode": "snapshot",
    "mail_provider": "mailtm",
    "mail_config": {"api_base": "https://api.mail.tm", "api_key": "", "bearer_token": ""},
    "sub2api_min_candidates": 200,
    "sub2api_auto_maintain": False,
    "sub2api_maintain_interval_minutes": 30,
    "proxy": "",
    "auto_register": False,
    "proxy_pool_enabled": True,
    "proxy_pool_api_url": "https://zenproxy.top/api/fetch",
    "proxy_pool_auth_mode": "query",
    "proxy_pool_api_key": "",
    "proxy_pool_count": 1,
    "proxy_pool_country": "US",
}


def _sort_key_from_filename(f: str) -> int:
    """从 Token 文件名中提取时间戳用于排序"""
    m = re.search(r'_(\d{10,})\.json$', f)
    return int(m.group(1)) if m else 0


# ==========================================
# FileStorage —— 本地文件系统
# ==========================================

class FileStorage(StorageBackend):
    """本地文件存储，与原有行为完全一致"""

    def __init__(self, data_dir: Path):
        self._data_dir = data_dir
        self._tokens_dir = data_dir / "tokens"
        self._config_file = data_dir / "sync_config.json"
        self._state_file = data_dir / "state.json"

        # 确保目录存在
        self._data_dir.mkdir(exist_ok=True)
        self._tokens_dir.mkdir(exist_ok=True)

    # ---- 配置 ----
    def load_config(self) -> Dict[str, Any]:
        if self._config_file.exists():
            try:
                return json.loads(self._config_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        return dict(_DEFAULT_CONFIG)

    def save_config(self, cfg: Dict[str, Any]) -> None:
        self._config_file.write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # ---- 统计 ----
    def load_state(self) -> Dict[str, int]:
        if self._state_file.exists():
            try:
                return json.loads(self._state_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"success": 0, "fail": 0}

    def save_state(self, success: int, fail: int) -> None:
        try:
            self._state_file.write_text(
                json.dumps({"success": success, "fail": fail}), encoding="utf-8"
            )
        except Exception:
            pass

    # ---- Token ----
    def save_token(self, filename: str, data: str) -> None:
        fpath = self._tokens_dir / filename
        fpath.write_text(data, encoding="utf-8")

    def load_token(self, filename: str) -> Optional[Dict[str, Any]]:
        fpath = self._tokens_dir / filename
        if not fpath.is_file():
            return None
        try:
            raw = json.loads(fpath.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else None
        except Exception:
            return None

    def load_token_raw(self, filename: str) -> Optional[str]:
        fpath = self._tokens_dir / filename
        if not fpath.is_file():
            return None
        try:
            return fpath.read_text(encoding="utf-8")
        except Exception:
            return None

    def list_tokens(self) -> List[str]:
        if not self._tokens_dir.is_dir():
            return []
        files = [f for f in os.listdir(self._tokens_dir) if f.endswith(".json")]
        files.sort(key=_sort_key_from_filename, reverse=True)
        return files

    def delete_token(self, filename: str) -> bool:
        fpath = self._tokens_dir / filename
        if fpath.is_file():
            fpath.unlink()
            return True
        return False

    def update_token(self, filename: str, data: Dict[str, Any]) -> bool:
        fpath = self._tokens_dir / filename
        if not fpath.is_file():
            return False
        try:
            fpath.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            return True
        except Exception:
            return False

    def token_exists(self, filename: str) -> bool:
        return (self._tokens_dir / filename).is_file()


# ==========================================
# RedisStorage —— Redis（适配 Upstash 等）
# ==========================================

class RedisStorage(StorageBackend):
    """
    Redis 存储后端。
    数据结构：
      orc:config           → STRING (JSON)
      orc:state            → STRING (JSON)
      orc:tokens:{filename} → STRING (token JSON)    每个 token 独立 key
      orc:tokens:index     → ZSET  filename → timestamp  用于排序列表
    """

    KEY_CONFIG = "orc:config"
    KEY_STATE = "orc:state"
    KEY_TOKEN_PREFIX = "orc:tokens:"
    KEY_TOKEN_INDEX = "orc:tokens:index"

    def __init__(self, redis_url: str):
        try:
            import redis as redis_lib
        except ImportError:
            raise RuntimeError(
                "Redis 后端需要安装 redis 包: pip install redis"
            )
        self._r = redis_lib.from_url(redis_url, decode_responses=True)
        # 测试连接
        try:
            self._r.ping()
        except Exception as e:
            raise RuntimeError(f"无法连接 Redis: {e}")

    def _token_key(self, filename: str) -> str:
        return f"{self.KEY_TOKEN_PREFIX}{filename}"

    # ---- 配置 ----
    def load_config(self) -> Dict[str, Any]:
        raw = self._r.get(self.KEY_CONFIG)
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                pass
        return dict(_DEFAULT_CONFIG)

    def save_config(self, cfg: Dict[str, Any]) -> None:
        self._r.set(self.KEY_CONFIG, json.dumps(cfg, ensure_ascii=False))

    # ---- 统计 ----
    def load_state(self) -> Dict[str, int]:
        raw = self._r.get(self.KEY_STATE)
        if raw:
            try:
                return json.loads(raw)
            except Exception:
                pass
        return {"success": 0, "fail": 0}

    def save_state(self, success: int, fail: int) -> None:
        try:
            self._r.set(self.KEY_STATE, json.dumps({"success": success, "fail": fail}))
        except Exception:
            pass

    # ---- Token ----
    def save_token(self, filename: str, data: str) -> None:
        pipe = self._r.pipeline()
        pipe.set(self._token_key(filename), data)
        # 用文件名中的时间戳作为排序分数
        score = _sort_key_from_filename(filename) or time.time_ns()
        pipe.zadd(self.KEY_TOKEN_INDEX, {filename: score})
        pipe.execute()

    def load_token(self, filename: str) -> Optional[Dict[str, Any]]:
        raw = self._r.get(self._token_key(filename))
        if raw:
            try:
                data = json.loads(raw)
                return data if isinstance(data, dict) else None
            except Exception:
                return None
        return None

    def load_token_raw(self, filename: str) -> Optional[str]:
        return self._r.get(self._token_key(filename))

    def list_tokens(self) -> List[str]:
        # ZREVRANGE 按分数降序（最新在前）
        try:
            return self._r.zrevrange(self.KEY_TOKEN_INDEX, 0, -1)
        except Exception:
            return []

    def delete_token(self, filename: str) -> bool:
        pipe = self._r.pipeline()
        pipe.delete(self._token_key(filename))
        pipe.zrem(self.KEY_TOKEN_INDEX, filename)
        results = pipe.execute()
        return bool(results[0])

    def update_token(self, filename: str, data: Dict[str, Any]) -> bool:
        if not self._r.exists(self._token_key(filename)):
            return False
        try:
            self._r.set(
                self._token_key(filename),
                json.dumps(data, ensure_ascii=False),
            )
            return True
        except Exception:
            return False

    def token_exists(self, filename: str) -> bool:
        return bool(self._r.exists(self._token_key(filename)))


# ==========================================
# 工厂函数
# ==========================================

def get_storage(data_dir: Optional[Path] = None) -> StorageBackend:
    """
    根据环境变量创建存储后端实例。

    环境变量：
      STORAGE_BACKEND = file | redis  (默认 file)
      REDIS_URL       = redis://...   (redis 后端必填)
    """
    backend = os.environ.get("STORAGE_BACKEND", "file").strip().lower()

    if backend == "redis":
        redis_url = os.environ.get("REDIS_URL", "").strip()
        if not redis_url:
            raise RuntimeError("STORAGE_BACKEND=redis 时必须设置 REDIS_URL 环境变量")
        print(f"[Storage] 使用 Redis 后端: {redis_url[:30]}...")
        return RedisStorage(redis_url)
    else:
        if data_dir is None:
            # 默认使用包目录上一级的 data/
            from . import PROJECT_ROOT
            data_dir = PROJECT_ROOT / "data"
        print(f"[Storage] 使用文件后端: {data_dir}")
        return FileStorage(data_dir)
