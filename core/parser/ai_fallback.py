"""
ai_fallback.py
AI-based fallback parser using OpenAI for complex/edge case signals
"""
import os
import re
import json
from typing import Optional
from utils.logger import get_logger

# Import from parent package
from . import ParsedSignal
from .pattern_parsers import is_scalp
from .validators import validate_signal

logger = get_logger("parser.ai_fallback")


class AIFallbackParser:
    """
    AI-based fallback parsing strategy using OpenAI

    This parser is used when pattern-based parsing fails.
    It handles edge cases like:
    - Typos or malformed numbers
    - Wrong/missing symbols
    - Mixed formats
    - Unusual spacing or ordering
    """

    def __init__(self, channel_config: dict = None):
        self.channel_config = channel_config or {}
        self.api_key = os.getenv('OPENAI_API_KEY')
        self.model = os.getenv('OPENAI_MODEL', 'gpt-4o-mini')
        self.enabled = bool(self.api_key)

        if not self.enabled:
            logger.warning("OpenAI API key not found, AI parsing disabled")
        else:
            logger.info(f"Initialized AIFallbackParser with model {self.model}")

    def parse(self, message: str, channel_name: str = None) -> Optional[ParsedSignal]:
        """
        Parse using AI

        Args:
            message: The message to parse
            channel_name: Channel name for context

        Returns:
            ParsedSignal or None
        """
        if not self.enabled:
            return None

        try:
            import openai

            # Create prompt
            prompt = self._create_prompt(message, channel_name)

            # Call OpenAI API
            client = openai.OpenAI(api_key=self.api_key)

            # Build request parameters
            request_params = {
                "model": self.model,
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a trading signal parser fallback. Extract trading information from messages."
                    },
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.1
            }

            # Use appropriate token parameter based on model
            if self.model.startswith(('gpt-4o', 'gpt-4-turbo', 'gpt-3.5-turbo-0125')):
                request_params["max_completion_tokens"] = 500
            else:
                request_params["max_tokens"] = 500

            response = client.chat.completions.create(**request_params)

            # Parse response
            ai_text = response.choices[0].message.content
            return self._parse_ai_response(ai_text, message, channel_name)

        except Exception as e:
            logger.error(f"AI parsing error: {e}")
            return None

    def _create_prompt(self, message: str, channel_name: str) -> str:
        """Create prompt for AI parsing"""
        # Determine channel context
        channel_context = self._get_channel_context(channel_name)

        prompt = f"""You are the fallback parser for a trading signal.
The main high-confidence parser FAILED, meaning this message is likely to be an edge case with:
- Typos or malformed numbers
- Wrong/missing symbols
- Mixed formats
- Unusual spacing or ordering

Your job is to parse and correct issues **when confident**.
If you cannot confidently correct an issue, return `null`.

Message: "{message}"
Channel: {channel_name or 'unknown'}
{channel_context}

Extract:
1. Instrument/Symbol (forex pair, commodity, index, crypto, or stock)
2. Direction (long/buy or short/sell)
3. Entry limits (list of price levels for entry)
4. Stop loss (single price level)
5. Expiry (VTH=week_end, VTAI/alien=no_expiry, VTD=day_end, VTWE=week_end, VTME=month_end)
6. Keywords (hot, semi-swing, scalp, intraday, position, etc.)

Guidelines for Robust Parsing:
- Treat this as an **edge case cleanup job**, not normal parsing.
- Look for relative absurdities:  
  * If one limit is hundreds of pips away while others are close together, it may be a typo – try to fix.  
  * If decimal placement seems off compared to other numbers in the same message, adjust accordingly.  
  * If one digit seems extra or missing and fixing it would make the set consistent, fix it.  
- Prefer internal consistency over absolute market knowledge (data may be outdated).
- If instrument is unclear but price pattern clearly matches a known instrument in context, infer it.
- If expiry keywords are slightly misspelled, correct them.
- Only return null when:
  * Symbol, direction, and both limits & stop loss cannot be confidently determined after reasonable corrections.
  * Multiple conflicting possibilities remain after trying to resolve.
- **DO NOT PARSE IF IT'S A FUTURES SIGNAL. RETURN NULL**
- **"vth" in text means week_end**

Stock-specific rules:
- Stock symbols end with .NYSE or .NAS
- Match company names to tickers only if price pattern makes sense in context.

Non-stock defaults:
- Gold: XAUUSD if no instrument
- Oil: USOILSPOT unless "IC" mentioned (then XTIUSD)
- Indices: SPX/SP500=SPX500USD, NAS/NASDAQ=NAS100USD, DOW/US30=US30USD
- Crypto ends with USDT (BTC=BTCUSDT, etc.)

Return format (JSON):
{{
  "instrument": "SYMBOL",
  "direction": "long/short",
  "limits": [1.234, 1.235],
  "stop_loss": 1.230,
  "expiry": "day_end/week_end/month_end/no_expiry",
  "keywords": ["hot"]
}}

If unable to confidently parse even after correction attempts, return null.
"""

        return prompt

    def _get_channel_context(self, channel_name: str) -> str:
        """Get channel-specific context for the prompt"""
        if not channel_name:
            return ""

        channel_lower = channel_name.lower()

        # Check config first
        if channel_name in self.channel_config:
            settings = self.channel_config[channel_name]
            default_inst = settings.get("default_instrument")
            default_exp = settings.get("default_expiry")
            if default_inst:
                return (
                    f"Default instrument for this channel: {default_inst}. "
                    f"Default expiry for this channel: {default_exp}."
                )

        # Fallback to name-based detection
        if 'gold' in channel_lower:
            return (
                "This is from the gold channel - default instrument is XAUUSD if not specified. "
                "Default expiry for this channel is week_end."
            )
        elif 'oil' in channel_lower:
            return (
                "This is from the oil channel - default is USOILSPOT (or XTIUSD if 'IC' mentioned). "
                "Default expiry for this channel is week_end."
            )
        elif 'exotic' in channel_lower:
            return (
                "This is from a forex exotics channel - look for currency pairs. "
                "Default expiry for this channel is week_end."
            )
        elif 'crypto' in channel_lower:
            return (
                "This is from a crypto channel - symbols end with USDT. "
                "Default expiry for this channel is week_end."
            )
        elif 'indices' in channel_lower or 'index' in channel_lower:
            return (
                "This is from an indices channel - look for index symbols. "
                "Default expiry for this channel is week_end."
            )
        elif 'stock' in channel_lower:
            return (
                "This is from a stock channel - symbols end with .NYSE or .NAS. "
                "Default expiry for this channel is month_end."
            )
        elif 'ot-trade' in channel_lower:
            return "Default expiry for this channel is day_end."
        elif 'alt' in channel_lower:
            return (
                "This is from a crypto alts channel - symbols end with USDT. "
                "Default expiry for this channel is month_end."
            )

        return ""

    def _parse_ai_response(self, ai_text: str, original_message: str,
                           channel_name: str) -> Optional[ParsedSignal]:
        """Parse the AI response into a ParsedSignal"""
        logger.debug(f"AI Raw Response:\n{ai_text}")

        try:
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', ai_text, re.DOTALL)
            if not json_match:
                logger.debug("No JSON found in AI response")
                return None

            data = json.loads(json_match.group())

            if not data or data == "null":
                logger.debug("AI returned null")
                return None

            # Create signal
            signal = ParsedSignal(
                instrument=data.get('instrument'),
                direction=data.get('direction'),
                limits=data.get('limits', []),
                stop_loss=data.get('stop_loss'),
                expiry_type=data.get('expiry', 'day_end'),
                raw_text=original_message,
                parse_method='ai',
                keywords=data.get('keywords', []),
                channel_name=channel_name,
                scalp=is_scalp(original_message, channel_name)
            )

            # Validate before returning
            if validate_signal(signal):
                logger.info(f"AI parse success: {signal.instrument} {signal.direction}")
                return signal

            logger.debug("AI signal failed validation")
            return None

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI JSON response: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing AI response: {e}")
            return None