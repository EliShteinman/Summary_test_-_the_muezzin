import whisper
import logging
import asyncio

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
    def whisper_transcribe(self, file_path, file_hash: str):
        audio = whisper.load_audio(file_path)
        audio = whisper.pad_or_trim(audio)
        mel = whisper.log_mel_spectrogram(audio, n_mels=self.model.dims.n_mels).to(self.model.device)
        _, probs = self.model.detect_language(mel)
        logger.info(f"Detected language: {max(probs, key=probs.get)}")
        options = whisper.DecodingOptions()
        result = whisper.decode(self.model, mel, options)
        logger.debug(f"Transcription: {result.text}")

        return result.text, file_hash


if __name__ == "__main__":
    whisper_service = WhisperService(
        model_name="tiny",
        download_root=r"C:\models\whisper"
    )

    text, file_hash = whisper_service.whisper_transcribe(
            r"C:\podcasts\download (6).wav",
            "jhgyuftydtuyytftrdtr"
    )
    print(text)
    print(file_hash)