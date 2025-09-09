import whisper
import logging

logger = logging.getLogger(__name__)


class WhisperService:
    def __init__(
            self,
            model_name: str,
            download_root: str
    ):
        self.model = whisper.load_model(
            name=model_name,
            download_root=download_root
        )

    def whisper_transcribe(self, file_path, file_hash: str, **kwargs):
        result = whisper.transcribe(
            model=self.model,
            audio=file_path,
            fp16=False,
            word_timestamps=True,
            **kwargs
        )

        logger.info(f"Detected language: {result['language']}")
        logger.debug(f"Transcription completed for: {file_hash}")

        return {
            'file_hash': file_hash,
            'full_text': result['text'],
            'language': result['language'],
            'segments': result['segments']
        }


if __name__ == "__main__":
    from pprint import pprint
    whisper_service = WhisperService(
        model_name="tiny",
        download_root=r"C:\models\whisper"
    )

    result = whisper_service.whisper_transcribe(
            r"C:\podcasts\download (6).wav",
            "jhgyuftydtuyytftrdtr"
    )
    pprint(result.keys())