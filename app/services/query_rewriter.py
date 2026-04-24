"""
services/query_rewriter.py  –  LLM-based Query Rewriting Layer

Stage 0 in the pipeline — runs BEFORE query_parser.py.

The user can be as messy, ambiguous, Hinglish, or typo-ridden as they want.
This layer's ONLY job is to take that garbage and produce a single clean,
unambiguous English sentence that the parser can handle perfectly.

Why a second LLM call instead of a dictionary:
  • Handles ANY spelling error, not just predefined ones
  • Understands context, intent, and domain
  • Resolves mixed language (Hinglish, regional terms)
  • Infers what the user MEANT, not just what they typed
  • Handles pronouns, ellipsis, and follow-up queries correctly
  • Handles multi-intent queries ("total cost AND received for X")

The rewriter is given the live schema so it can map casual column references
to real column names. It does NOT produce JSON — only clean English.

AI DOES NOT TOUCH THIS FILE (except the system prompt which it executes).
"""
from __future__ import annotations
import time
from app.services.llm_client import get_llm_client
from app.utils.logger import get_logger

logger = get_logger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Rewriter system prompt
# ─────────────────────────────────────────────────────────────────────────────

def _build_rewriter_prompt(schema: dict[str, dict]) -> str:
    """
    Build a focused system prompt that gives the rewriter just enough schema
    context to resolve column aliases and tab names — nothing more.
    """

    # Build a compact schema summary (just names + types, no values)
    schema_lines: list[str] = []
    for tab, cols in schema.items():
        schema_lines.append(f'\nTab: "{tab}"')
        for col, meta in cols.items():
            t = meta.get("type", "?") if isinstance(meta, dict) else meta
            schema_lines.append(f'  - "{col}"  ({t})')

    schema_block = "\n".join(schema_lines)

    return f"""You are a QUERY REWRITER for a real-estate payment tracking bot.
Your ONLY job: take any messy user message and rewrite it as one clean,
unambiguous English sentence that a strict JSON query parser can understand.

════════════════════════════════════════════
LIVE GOOGLE SHEET SCHEMA
════════════════════════════════════════════
{schema_block}
════════════════════════════════════════════

REWRITING RULES
═══════════════

1. FIX ALL SPELLING — correct any typo, abbreviation, or phonetic spelling.
   Examples:
     "toal recevied fro cogers"   →  "total received amount for group COGERS"
     "balanc of H-012"            →  "balance amount for apartment H-012"
     "hw many custmers in phase2" →  "how many customers are in phase 2"
     "rcvd % aman"                →  "what is the received percentage for group AMAN"

2. RESOLVE HINGLISH / REGIONAL TERMS — translate to clear English.
   Examples:
     "kitne log hain phase 1 mein"     →  "how many customers are in phase 1"
     "L-044 ka total aur received kya" →  "what is the total cost and received amount for apartment L-044"
     "ye wala group ka total"          →  if context is clear → resolve; else → ask

3. EXPAND ABBREVIATIONS — map to the nearest real column name.
   Use the schema to map casual terms to actual column names:
     "total cost" / "sale price" / "total amount" → mention "total sale value"
     "received" / "rcvd" / "amount got"           → mention "received amount"
     "balance" / "pending" / "baaki"              → mention "balance"
     "% received" / "recovery %" / "kitna mila %"→ mention "received percentage"
     "court" / "case" / "legal"                   → mention "court case"

4. SPLIT MULTI-INTENT QUERIES — if the user asks for TWO numeric values:
   Keep them in ONE sentence using "and":
     "total cost and received for L-044" →
         "what is the total sale value and received amount for apartment L-044"
   Do NOT split into two sentences. Do NOT choose one and drop the other.

5. RESOLVE PRONOUNS — if "his", "her", "it", "the same", "above" appear and
   the name/ID is MISSING from the current message:
   Output EXACTLY: "CLARIFY: I don't have context from previous messages.
   Please include the name or apartment number."

6. PRESERVE EXACT IDENTIFIERS — apartment codes (H-012, L-044, S-012),
   names (GAURAV KHANNA, COGERS), and serial numbers must be kept EXACTLY as
   the user typed them. Do not correct or normalize proper nouns.

7. DO NOT ANSWER THE QUESTION — you are a rewriter, not an answering machine.
   Never say "The total is..." or "I found...". Only output the clean query.

8. DO NOT ADD FILTERS NOT IN THE ORIGINAL — if the user asks for "total cost
   for AMAN", do not add "and received amount" just because they go together.
   Rewrite what the user asked, nothing more.

9. HANDLE NONSENSE GRACEFULLY — if the message is truly unrelated to the
   database (jokes, greetings handled separately, random text):
   Output EXACTLY: "UNRELATED: <one sentence saying what you think they meant>"

OUTPUT FORMAT
═════════════
Return ONLY the rewritten query as a single clean English sentence.
No preamble. No explanation. No JSON. No markdown.
Just the sentence.

Examples:
  Input:  "toal amnt receied cogers?"
  Output: "what is the total received amount for group COGERS"

  Input:  "L044 ka kitna paid hai aur total"
  Output: "what is the received amount and total sale value for apartment L-044"

  Input:  "how many units r thr with pending more then 50%"
  Output: "how many units have balance amount greater than 50 percent"

  Input:  "is tilgota in nclt court case?"
  Output: "is there a court case for customer tilgota in NCLT"

  Input:  "amount from M-105"
  Output: "what is the received amount for apartment M-105"

  Input:  "show his details"
  Output: "CLARIFY: I don't have context from previous messages. Please include the name or apartment number."
"""


# ─────────────────────────────────────────────────────────────────────────────
# QueryRewriter
# ─────────────────────────────────────────────────────────────────────────────

class QueryRewriter:
    """
    Stage 0 of the query pipeline.

    Takes any user message → returns a clean English query string.
    The downstream parser (query_parser.py) then converts THAT to JSON.
    """

    async def rewrite(
        self,
        raw_message: str,
        schema: dict[str, dict],
    ) -> tuple[str, bool]:
        """
        Returns:
          (rewritten_query, needs_clarification)

        If needs_clarification is True, rewritten_query is a user-facing
        message to send back (starts with "CLARIFY:" or "UNRELATED:").
        """
        t0            = time.monotonic()
        system_prompt = _build_rewriter_prompt(schema)
        user_prompt   = raw_message.strip()

        client = get_llm_client()
        try:
            result = await client.complete(system_prompt, user_prompt)
        except Exception as exc:
            logger.warning(
                "rewriter_llm_failed_passthrough",
                error=str(exc),
                raw=raw_message[:80],
            )
            # Fail open: pass original message to parser unchanged
            return raw_message, False

        result  = result.strip()
        elapsed = round((time.monotonic() - t0) * 1000, 1)

        # Detect special output tokens
        if result.startswith("CLARIFY:"):
            msg = result[len("CLARIFY:"):].strip()
            logger.info("rewriter_clarification_needed", ms=elapsed, raw=raw_message[:60])
            return msg, True

        if result.startswith("UNRELATED:"):
            msg = result[len("UNRELATED:"):].strip()
            logger.info("rewriter_unrelated_query", ms=elapsed, raw=raw_message[:60])
            return msg, True

        logger.info(
            "rewriter_success",
            ms=elapsed,
            original=raw_message[:60],
            rewritten=result[:80],
        )
        return result, False


    async def rewrite_with_fallback(
        self,
        raw_message: str,
        schema: dict[str, dict],
    ) -> tuple[str, bool]:
        """
        Same as rewrite() but catches ALL exceptions and always returns
        something the pipeline can continue with.
        """
        try:
            return await self.rewrite(raw_message, schema)
        except Exception as exc:
            logger.error("rewriter_unexpected_error", error=str(exc))
            return raw_message, False


# ─────────────────────────────────────────────────────────────────────────────
_rewriter: QueryRewriter | None = None


def get_rewriter() -> QueryRewriter:
    global _rewriter
    if _rewriter is None:
        _rewriter = QueryRewriter()
    return _rewriter