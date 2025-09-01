from dataclasses import dataclass
import os

@dataclass
class Settings:
    BOT_TOKEN: str = os.environ["REPORT_BOT_TOKEN"]

    # Bitrix
    BITRIX_WEBHOOK_BASE: str = os.environ["BITRIX_WEBHOOK_BASE"]  # https://<domain>.bitrix24.ua/rest/xxx/yyy
    B24_DOMAIN: str = os.environ["B24_DOMAIN"]                    # fiberlink.bitrix24.ua

    # Webhook
    WEBHOOK_BASE: str = os.environ["WEBHOOK_BASE"]                # https://<app>.fly.dev
    WEBHOOK_SECRET: str = os.environ.get("WEBHOOK_SECRET", "sec")

    # Куди слати звіт по кожній бригаді
    # chat_id може бути груповим (від’ємні значення) або приватним
    BRIGADE_CHAT_1: int = int(os.environ["BRIGADE_CHAT"])

    # Хто може виконувати /report_now (через пробіл, комами або один id)
    ADMIN_IDS_RAW: str = os.environ.get("ADMIN_IDS", "")

    # Анти-спам/ретраї
    TG_TIMEOUT: float = float(os.environ.get("TG_TIMEOUT", "10"))
    B24_TIMEOUT: float = float(os.environ.get("B24_TIMEOUT", "60"))

    def admins(self) -> set[int]:
        if not self.ADMIN_IDS_RAW.strip():
            return set()
        parts = [p.strip() for p in self.ADMIN_IDS_RAW.replace(",", " ").split()]
        return {int(p) for p in parts if p}

settings = Settings()
