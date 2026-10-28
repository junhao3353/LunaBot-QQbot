# -*- coding: utf-8 -*-
"""抖音解析服务（DTK v5 / douyin_tiktok_download_api）的瘦客户端。

DTK v5 架构：
  · 解析：POST /api/v1/parse → 任务ID，轮询 GET /api/v1/tasks/{id} 拿结果
  · 结果里直接带视频/图片直链（data.data.media.video.url / images）
  · 旧版 /api/v1/downloads 下载功能在 v5 需独立 downloader 侧车，
    本模块不依赖它——拿到直链后自己用 requests 下载到本地。

主程序接口保持不变：start_download / wait_download / pick_media / fetch_file
"""
from __future__ import annotations

import os
import ipaddress
import re
import socket
import time
from typing import Optional
from urllib.parse import urljoin, urlparse

import requests

_URL_RE = re.compile(r"https?://[^\s'\"<>]+")

# 只允许访问抖音/字节系官方域名（防 SSRF：群里发的链接会被本模块请求）
_ALLOWED_HOSTS = {
    # 抖音主域名
    "douyin.com", "www.douyin.com", "m.douyin.com", "v.douyin.com",
    "iesdouyin.com", "www.iesdouyin.com", "m.iesdouyin.com",
    # 字节系CDN（DTK返回的视频/图片下载地址）
    "bytecdntp.com", "douyinvod.com", "bytecdn.cn", "byteimg.com",
    "douyinpic.com", "pstatp.com", "toutiaoimg.com", "ixigua.com",
    "bytedance.com", "bytedance.net", "toutiao.com", "feiliao.com",
    "musical.ly", "amemv.com", "snssdk.com",
}


def _host_allowed(url: str) -> bool:
    """URL 的主机是否属于抖音官方域名（含子域名）"""
    try:
        host = (urlparse(url).hostname or "").lower().rstrip(".")
    except Exception:
        return False
    if not host:
        return False
    return host in _ALLOWED_HOSTS or any(host.endswith("." + h) for h in _ALLOWED_HOSTS)


# 媒体直链用的通用 SSRF 防护：允许公网任意 CDN（DTK 返回的域名经常不在官方白名单里），
# 只拦截内网/本机/保留地址，防止被当成探测内网的跳板。
_BAD_HOSTNAMES = {"localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback"}


def _url_is_public(url: str) -> bool:
    """URL 是否是可安全下载的公网地址（http/https + 主机能解析到公网 IP）"""
    try:
        p = urlparse(url)
        if p.scheme not in ("http", "https"):
            return False
        host = (p.hostname or "").lower().rstrip(".")
        if not host or host in _BAD_HOSTNAMES or host.endswith(".local") or host.endswith(".internal"):
            return False
        # 主机本身就是 IP：直接判断
        try:
            ip = ipaddress.ip_address(host)
            return not (ip.is_private or ip.is_loopback or ip.is_link_local
                        or ip.is_reserved or ip.is_multicast or ip.is_unspecified)
        except ValueError:
            pass
        # 域名：解析后所有地址都必须是公网
        try:
            infos = socket.getaddrinfo(host, None)
        except Exception:
            return False
        if not infos:
            return False
        for info in infos:
            try:
                ip = ipaddress.ip_address(info[4][0])
            except Exception:
                return False
            if (ip.is_private or ip.is_loopback or ip.is_link_local
                    or ip.is_reserved or ip.is_multicast or ip.is_unspecified):
                return False
        return True
    except Exception:
        return False


def _safe_get(url: str, headers: dict, timeout: float, max_redirects: int = 5):
    """手动跟随跳转，每一跳都校验公网地址，避免 302 到内网（SSRF）"""
    cur = url
    for _ in range(max_redirects + 1):
        if not _url_is_public(cur):
            raise ValueError("下载地址不是公网地址")
        r = requests.get(cur, headers=headers, timeout=timeout, stream=True, allow_redirects=False)
        if r.status_code in (301, 302, 303, 307, 308) and r.headers.get("Location"):
            loc = r.headers["Location"]
            try:
                r.close()
            except Exception:
                pass
            cur = urljoin(cur, loc)
            continue
        return r
    raise ValueError("跳转次数过多")


# 下载抖音资源时用的请求头（不带 Referer 抖音会 403）
_DL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36 Edg/152.0.0.0",
    "Referer": "https://www.douyin.com/",
    "Accept": "*/*",
}

# fetch_file 需要的 URL 缓存（接口签名里没有 url 参数，用 download_id+name 做 key）
_url_cache: dict = {}


def _extract_url(text: str) -> str:
    """从分享文案里抠出链接（抖音分享常带一堆文字）"""
    m = _URL_RE.search(text or "")
    return m.group(0) if m else (text or "").strip()


def _headers(api_key: str) -> dict:
    h = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    if api_key:
        h["X-API-Key"] = api_key
        h["Authorization"] = f"Bearer {api_key}"
    return h


def _first(d: dict, keys) -> Optional[str]:
    for k in keys:
        v = d.get(k)
        if isinstance(v, (str, int)) and str(v).strip():
            return str(v).strip()
    return None


def _poll_task(base: str, api_key: str, task_id: str, *,
               timeout: float = 120.0, interval: float = 2.0) -> dict:
    """轮询 GET /api/v1/tasks/{id} 直到任务结束，返回 {"state":..., "result":..., "err":...}"""
    base = base.rstrip("/")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{base}/api/v1/tasks/{task_id}",
                             headers=_headers(api_key), timeout=15)
        except Exception:
            time.sleep(interval)
            continue
        if r.status_code != 200:
            time.sleep(interval)
            continue
        try:
            data = r.json() or {}
        except Exception:
            time.sleep(interval)
            continue
        payload = data.get("data", data) if isinstance(data, dict) else {}
        state = str(payload.get("state") or "").lower()
        if state in ("done", "completed", "success"):
            result = payload.get("data") or payload.get("result") or payload
            return {"state": "done", "result": result, "err": None}
        if state in ("failed", "error", "cancelled"):
            return {"state": state, "result": payload, "err": f"任务失败：{state}"}
        time.sleep(interval)
    return {"state": "timeout", "result": {}, "err": f"等待超时（{timeout:.0f}秒）"}


def _extract_media(result: dict) -> dict:
    """从 parse 结果里提取视频/图片直链。
    返回 {"kind":"video"|"image", "video_url":str, "image_urls":[str], "title":str}
    支持：视频、静态图、GIF动图、Live Photo（动态照片，从raw数据提取video）"""
    kind = str(result.get("kind") or "").lower()
    # 取文案，去掉 #hashtag 标签，只保留纯文字部分
    raw_title = str(result.get("title") or result.get("description") or result.get("desc") or "").strip()
    title = re.sub(r'#\S+', "", raw_title)  # 去掉 #标签
    title = re.sub(r'\s+', " ", title).strip()
    media = result.get("media") or {}
    # include_raw=true 时返回的抖音原始数据（含Live Photo视频）
    raw = result.get("raw") or {}

    # 视频
    video_url = ""
    v = media.get("video") or {}
    if isinstance(v, dict):
        video_url = v.get("url") or ""
        if not video_url and isinstance(v.get("urls"), list) and v["urls"]:
            video_url = v["urls"][0]

    # 图文（图片列表）：支持静态图、GIF动图、Live Photo（动态照片）
    image_urls = []
    live_photo_urls = []  # Live Photo动态视频，与image_urls按序号对应
    imgs = media.get("images") or media.get("image") or []
    # raw里的images含video字段（Live Photo动态视频）
    raw_imgs = raw.get("images") or []
    if isinstance(imgs, list):
        for idx, img in enumerate(imgs):
            if isinstance(img, dict):
                # DTK v5 返回 url + urls列表，优先取url
                u = img.get("url") or img.get("url_list") or ""
                if isinstance(u, list) and u:
                    u = u[0]
                if not u and isinstance(img.get("urls"), list) and img["urls"]:
                    u = img["urls"][0]
                if u:
                    image_urls.append(str(u))
                else:
                    image_urls.append("")  # 占位，保持序号对应
                # Live Photo：从raw数据提取该图对应的动态视频
                live_url = ""
                if idx < len(raw_imgs):
                    raw_img = raw_imgs[idx]
                    if isinstance(raw_img, dict):
                        rv = raw_img.get("video") or {}
                        if isinstance(rv, dict):
                            pa = rv.get("play_addr") or {}
                            if isinstance(pa, dict) and isinstance(pa.get("url_list"), list) and pa["url_list"]:
                                live_url = str(pa["url_list"][0])
                live_photo_urls.append(live_url)  # 与image_urls同序号，无则空串
            elif isinstance(img, str) and img.startswith("http"):
                image_urls.append(img)
                live_photo_urls.append("")

    if not kind:
        kind = "video" if video_url else ("image" if image_urls else "unknown")
    # image_album 是图文，统一当作 image 处理
    if kind == "image_album":
        kind = "image"
    return {"kind": kind, "video_url": video_url, "image_urls": image_urls,
            "live_photo_urls": live_photo_urls, "title": title}


def _register_urls(download_id: str, files: list) -> None:
    """把 files 里的 url 注册到全局缓存，供 fetch_file 使用（超过500条时淘汰最旧的）"""
    for f in files:
        if isinstance(f, dict) and f.get("url") and f.get("name"):
            _url_cache[(download_id, f["name"])] = f["url"]
    if len(_url_cache) > 500:
        for k in list(_url_cache.keys())[:len(_url_cache) - 400]:
            _url_cache.pop(k, None)


def _safe_filename(name: str, max_len: int = 80) -> str:
    """清理文件名中的非法字符，截断到指定长度。"""
    name = re.sub(r'[\\/:*?"<>|\r\n\t]', " ", name)
    name = re.sub(r'\s+', " ", name).strip()
    if len(name) > max_len:
        name = name[:max_len]
    return name


# ---------------------------------------------------------------------------
# 主程序调用的公开接口（保持签名不变）
# ---------------------------------------------------------------------------

def resolve_short_link(base: str, api_key: str, link: str, *, timeout: float = 15.0,
                       poll_interval: float = 2.0, poll_limit: int = 30) -> str:
    """短链展开：DTK v5 的 parse 直接接受短链；这里只做"安全兜底"。
    安全约束（防 SSRF）：只允许访问抖音官方域名，且不跟随跳转，拿到 Location 后再校验域名。"""
    link = _extract_url(link)
    if "v.douyin.com" not in link and "/share/" not in link:
        return link
    if not _host_allowed(link):
        # 域名不在白名单（例如 evil-douyin.com）→ 绝不去请求
        return link
    try:
        r = requests.get(link, headers={"User-Agent": _DL_HEADERS["User-Agent"]},
                         timeout=timeout, allow_redirects=False)
        loc = r.headers.get("Location") or ""
        if loc and _host_allowed(loc) and "douyin.com" in loc:
            return loc
    except Exception:
        pass
    return link


def start_download(base: str, api_key: str, link: str, *, timeout: float = 20.0) -> dict:
    """提交解析任务（POST /api/v1/parse）。
    返回 {"download_id":task_id, "state":"pending", "err":None|str}"""
    base = (base or "").rstrip("/")
    if not base:
        return {"err": "未配置 douyin_api_base"}
    link = _extract_url(link)
    try:
        r = requests.post(f"{base}/api/v1/parse", json={"url": link, "include_raw": True},
                          headers=_headers(api_key), timeout=timeout)
    except Exception as e:
        return {"err": f"提交失败：{e}"}
    if r.status_code not in (200, 202):
        return {"err": f"提交失败 HTTP {r.status_code}：{(r.text or '')[:160]}"}
    try:
        data = r.json() or {}
    except Exception:
        return {"err": "解析服务返回不是JSON"}
    payload = data.get("data", data) if isinstance(data, dict) else {}
    task_id = _first(payload, ("task_id", "id", "job_id"))
    if not task_id:
        return {"err": f"返回里没有 task_id：{str(payload)[:160]}"}
    return {
        "download_id": task_id,
        "state": _first(payload, ("state",)) or "pending",
        "directory": "",
        "err": None,
    }


def wait_download(base: str, api_key: str, download_id: str, *, timeout: float = 300.0,
                  interval: float = 3.0) -> dict:
    """轮询任务直到完成，从 parse 结果提取媒体直链。
    返回 {"state":"done", "files":[{"name":..., "url":..., "state":"done"}],
          "directory":"", "err":None|str}"""
    base = (base or "").rstrip("/")
    poll = _poll_task(base, api_key, download_id, timeout=timeout, interval=interval)
    if poll.get("err"):
        return {"state": poll.get("state", "error"), "files": [], "directory": "",
                "err": poll["err"]}
    media = _extract_media(poll["result"])
    # 用抖音文案作为文件名
    base_name = _safe_filename(media.get("title") or "") or "douyin"

    files = []
    if media["kind"] == "video" and media["video_url"]:
        files.append({"name": f"{base_name}.mp4", "url": media["video_url"], "state": "done"})
    elif media["kind"] == "image" and media["image_urls"]:
        live_urls = media.get("live_photo_urls") or []
        for i, u in enumerate(media["image_urls"]):
            if not u:
                continue
            ext = ".jpg"
            low = u.lower()
            if ".gif" in low:
                ext = ".gif"
            elif ".webp" in low:
                ext = ".webp"
            elif ".png" in low:
                ext = ".png"
            suffix = f"{i+1:02d}"  # 图文作品总是有序号
            files.append({"name": f"{suffix}{ext}", "url": u, "state": "done"})
            # Live Photo动态视频：与静态图同序号，命名为 xx_动态.mp4
            if i < len(live_urls) and live_urls[i]:
                files.append({"name": f"{suffix}_动态.mp4", "url": live_urls[i], "state": "done"})
    if not files:
        return {"state": "done", "files": [], "directory": "",
                "err": "解析成功但未提取到视频/图片链接（可能是图文或已删除作品）"}
    # 注册 URL 缓存供 fetch_file 使用
    _register_urls(download_id, files)
    return {"state": "done", "files": files, "directory": "", "err": None, "title": base_name}


def fetch_file(base: str, api_key: str, download_id: str, name: str, dest_path: str,
               *, media_dir: str = "", directory: str = "", timeout: float = 300.0,
               max_mb: float = 200.0) -> dict:
    """把文件下载到本地（从 wait_download 注册的 URL 缓存里取直链）。
    media_dir / directory 参数保留仅为接口兼容，DTK v5 模式下不使用。
    安全约束：下载大小上限 max_mb（防止恶意/异常链接写满磁盘）、超限即中止并删除半成品。"""
    url = _url_cache.get((download_id, name))
    if not url:
        return {"err": f"找不到 {name} 的下载链接（内部缓存丢失）"}
    if not _url_is_public(url):
        return {"err": "资源地址不是公网地址（内网/本机/非法），已拒绝下载"}
    limit_bytes = int(max(1.0, float(max_mb)) * 1024 * 1024)
    try:
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        with _safe_get(url, _DL_HEADERS, timeout) as r:
            if r.status_code != 200:
                return {"err": f"下载失败 HTTP {r.status_code}"}
            try:
                declared = int(r.headers.get("Content-Length") or 0)
            except Exception:
                declared = 0
            if declared and declared > limit_bytes:
                return {"err": f"文件过大（{declared / 1048576:.1f}MB > 上限 {max_mb}MB），已拒绝下载"}
            written = 0
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    written += len(chunk)
                    if written > limit_bytes:
                        f.close()
                        try:
                            os.remove(dest_path)
                        except Exception:
                            pass
                        return {"err": f"下载超过上限 {max_mb}MB，已中止并清理"}
                    f.write(chunk)
        if os.path.getsize(dest_path) < 1024:
            return {"err": "下载的文件过小（可能被抖音拦截）"}
        return {"path": dest_path, "err": None}
    except Exception as e:
        try:
            if os.path.exists(dest_path):
                os.remove(dest_path)
        except Exception:
            pass
        return {"err": f"下载异常：{e}"}


def pick_media(files) -> dict:
    """从任务文件列表里判断是视频还是图文，返回 {"video":name, "images":[names]}"""
    video, images = None, []
    for f in files or []:
        name = str(f.get("name") or "")
        if not name:
            continue
        low = name.lower()
        is_live_photo = "_动态" in low
        if low.endswith((".mp4", ".mov", ".mkv", ".flv", ".webm")) and video is None and not is_live_photo:
            video = name
        elif low.endswith((".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic", ".mp4", ".mov")):
            images.append(name)
    return {"video": video, "images": images}
