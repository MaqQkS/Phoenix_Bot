"""
modules/telegram_sender.py
Formats and sends dip alert messages to Telegram.
"""

import aiohttp
import logging
from models import TrackedToken

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

# Tier styling: (color dots, tagline)
TIER_STYLE = {
    "Tier 1": {
        "dots": "🟡🟡🟡",
        "tagline": "🔪 Knife Catch 🔪",
    },
    "Tier 2": {
        "dots": "🟠🟠🟠🟠🟠",
        "tagline": "💰💰 DCA Opportunity 💰💰",
    },
    "Tier 3": {
        "dots": "🔴🔴🔴🔴🔴🔴🔴",
        "tagline": "😬😬😬😬😬 Pucker Up 😬😬😬😬😬",
    },
}


class TelegramSender:
    def __init__(self, config: dict):
        self.bot_token = config["telegram"]["bot_token"]
        self.chat_id   = config["telegram"]["chat_id"]

    async def send_dip_alert(
        self,
        token: TrackedToken,
        tier: dict,
        session: aiohttp.ClientSession,
    ):
        """Format and send a dip alert for a token."""
        msg = self._format_alert(token, tier)
        await self._send(msg, session)

    def _format_alert(self, token: TrackedToken, tier: dict) -> str:
        drop_pct    = token.drop_from_ath * 100
        mcap        = token.current_mcap
        ath_mcap    = token.ath_mcap or token.ath_price
        mig_mcap    = token.migration_mcap
        age_h       = token.age_hours

        # Volume spike label
        avg_1h = token.volume_6h / 6 if token.volume_6h > 0 else 0
        if avg_1h > 0 and token.volume_1h > avg_1h * 1.5:
            vol_label = "Spiking ⚡"
        elif token.volume_1h > 0:
            vol_label = "Active 📊"
        else:
            vol_label = "Quiet 😴"

        # Tier name and styling
        tier_name = tier.get("name", "Dip")
        style = TIER_STYLE.get(tier_name, {"dots": "⚠️", "tagline": ""})

        # Format mcap nicely
        def fmt_mcap(v: float) -> str:
            if v >= 1_000_000:
                return f"${v/1_000_000:.1f}M"
            elif v >= 1_000:
                return f"${v/1_000:.0f}k"
            else:
                return f"${v:.0f}"

        lines = [
            f"{style['dots']}",
            f"⚠️ Floor Watch — <b>${token.symbol}</b> [{tier_name} Dip]",
            f"📉 <b>{fmt_mcap(mcap)} MC</b> · 🚗 migrated at {fmt_mcap(mig_mcap)}",
            f"│ ATH: <b>{fmt_mcap(ath_mcap)}</b> · Floor: <b>-{drop_pct:.0f}% from peak</b>",
            f"│ Age: {age_h:.1f} hours",
            f"│ Vol: {vol_label}",
            f"",
            f"🔗 CA: <code>{token.address}</code>",
        ]

        if style["tagline"]:
            lines.append(f"")
            lines.append(f"{style['tagline']}")

        return "\n".join(lines)

    async def _send(self, text: str, session: aiohttp.ClientSession):
        url = TELEGRAM_API.format(token=self.bot_token)
        payload = {
            "chat_id":                  self.chat_id,
            "text":                     text,
            "parse_mode":               "HTML",
            "disable_web_page_preview": True,
        }
        try:
            async with session.post(
                url,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.info("✅ Telegram alert sent")
                else:
                    body = await resp.text()
                    logger.error(f"Telegram send failed {resp.status}: {body}")
        except Exception as e:
            logger.error(f"Telegram send error: {e}")

    async def send_startup_message(self, session: aiohttp.ClientSession):
        """Send a simple startup ping so you know the bot is alive."""
        await self._send("🤖 <b>Dip Bot v2 started</b> — watching for migrations...", session)