from app.services.media_mutation_service import MediaMutationService

_mutation_service = MediaMutationService()


def mutate_media_file_md5(file_path: str, mime_type: str | None, workdir: str) -> str:
    message = _build_message_stub(mime_type)
    return _mutation_service.mutate_file_md5(file_path=file_path, message=message, work_dir=workdir)


def _build_message_stub(mime_type: str | None):
    mime = str(mime_type or "").strip().lower()
    is_image = mime.startswith("image/")
    is_video = mime.startswith("video/")
    is_audio = mime.startswith("audio/")
    media_document = type("DocumentStub", (), {"mime_type": mime})()
    media = type("MediaStub", (), {"document": media_document, "photo": object() if is_image else None})()
    return type(
        "MessageStub",
        (),
        {
            "photo": object() if is_image else None,
            "video": is_video,
            "audio": is_audio,
            "voice": False,
            "media": media,
        },
    )()
