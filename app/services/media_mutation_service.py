import hashlib
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Any

try:
    from PIL import Image, ImageSequence, PngImagePlugin
except Exception:  # pragma: no cover - Pillow 缺失时安全降级
    Image = None
    ImageSequence = None
    PngImagePlugin = None


logger = logging.getLogger(__name__)


class MediaMutationService:
    @staticmethod
    def should_mutate(message: Any) -> bool:
        if getattr(message, "photo", None):
            return True

        document = getattr(getattr(message, "media", None), "document", None)
        mime_type = str(getattr(document, "mime_type", "") or "").lower()
        if mime_type.startswith("image/"):
            return True
        if mime_type.startswith("video/"):
            return True
        if mime_type.startswith("audio/"):
            return True
        return bool(getattr(message, "video", False) or getattr(message, "audio", False) or getattr(message, "voice", False))

    def mutate_file_md5(self, *, file_path: str, message: Any, work_dir: str) -> str:
        source_path = Path(file_path)
        if not source_path.exists() or not self.should_mutate(message):
            return str(source_path)

        if self._is_image_message(message):
            mutated = self._mutate_image(source_path=source_path, work_dir=Path(work_dir))
            return str(mutated or source_path)

        if self._is_av_message(message):
            mutated = self._mutate_av(source_path=source_path, work_dir=Path(work_dir))
            return str(mutated or source_path)

        return str(source_path)

    @staticmethod
    def _is_image_message(message: Any) -> bool:
        if getattr(message, "photo", None):
            return True
        document = getattr(getattr(message, "media", None), "document", None)
        mime_type = str(getattr(document, "mime_type", "") or "").lower()
        return mime_type.startswith("image/")

    @staticmethod
    def _is_av_message(message: Any) -> bool:
        document = getattr(getattr(message, "media", None), "document", None)
        mime_type = str(getattr(document, "mime_type", "") or "").lower()
        if mime_type.startswith("video/") or mime_type.startswith("audio/"):
            return True
        return bool(getattr(message, "video", False) or getattr(message, "audio", False) or getattr(message, "voice", False))

    @staticmethod
    def _build_mutation_token(source_path: Path) -> str:
        digest = hashlib.md5(f"{source_path.name}:{source_path.stat().st_size}".encode("utf-8"), usedforsecurity=False)
        return digest.hexdigest()

    def _mutate_image(self, *, source_path: Path, work_dir: Path) -> Path | None:
        if Image is None:
            return None

        token = self._build_mutation_token(source_path)
        output_path = work_dir / f"{source_path.stem}_md5{source_path.suffix.lower()}"
        try:
            with Image.open(source_path) as image:
                fmt = (image.format or source_path.suffix.lstrip(".") or "").upper()
                if fmt in {"JPEG", "JPG"}:
                    save_kwargs: dict[str, Any] = {
                        "format": "JPEG",
                        "quality": 95,
                        "optimize": False,
                        "comment": token.encode("utf-8"),
                    }
                    exif = image.info.get("exif")
                    if exif:
                        save_kwargs["exif"] = exif
                    image.save(output_path, **save_kwargs)
                elif fmt == "PNG":
                    png_info = PngImagePlugin.PngInfo() if PngImagePlugin is not None else None
                    if png_info is not None:
                        png_info.add_text("comment", token)
                    image.save(output_path, format="PNG", pnginfo=png_info)
                elif fmt == "GIF" and ImageSequence is not None:
                    frames = [frame.copy() for frame in ImageSequence.Iterator(image)]
                    if not frames:
                        return None
                    frames[0].save(
                        output_path,
                        format="GIF",
                        save_all=True,
                        append_images=frames[1:],
                        loop=image.info.get("loop", 0),
                        duration=image.info.get("duration"),
                        comment=token.encode("utf-8"),
                    )
                elif fmt == "WEBP":
                    image.save(output_path, format="WEBP", quality=95, method=6)
                else:
                    return None
        except Exception as exc:
            logger.warning("图片 MD5 改写失败，保留原文件: path=%s reason=%s", source_path, exc)
            return None

        if output_path.exists() and output_path.stat().st_size > 0:
            return output_path
        return None

    def _mutate_av(self, *, source_path: Path, work_dir: Path) -> Path | None:
        if not shutil.which("ffmpeg"):
            return None

        token = self._build_mutation_token(source_path)
        output_path = work_dir / f"{source_path.stem}_md5{source_path.suffix.lower()}"
        command = [
            "ffmpeg",
            "-y",
            "-i",
            str(source_path),
            "-map",
            "0",
            "-c",
            "copy",
            "-metadata",
            f"comment={token}",
        ]
        if source_path.suffix.lower() in {".mp4", ".m4v", ".mov"}:
            command.extend(["-movflags", "+faststart"])
        command.append(str(output_path))

        try:
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=120,
                check=False,
            )
        except Exception as exc:
            logger.warning("音视频 MD5 改写失败，保留原文件: path=%s reason=%s", source_path, exc)
            return None

        if result.returncode != 0:
            logger.warning(
                "音视频 MD5 改写失败，保留原文件: path=%s stderr=%s",
                source_path,
                (result.stderr or "")[:300],
            )
            return None

        if output_path.exists() and output_path.stat().st_size > 0:
            return output_path
        return None
