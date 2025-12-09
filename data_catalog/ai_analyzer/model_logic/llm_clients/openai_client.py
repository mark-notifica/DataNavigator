import os
import logging
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()

# Lazy client initialisatie zodat tests zonder OPENAI_API_KEY niet falen bij import.
_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None  # geen key → simulate modus gebruiken in analyze_with_openai
    try:
        _client = OpenAI(api_key=api_key)
    except Exception as e:
        logging.warning(f"[OPENAI] Kan client niet initialiseren: {e}; fallback naar simulate mode")
        _client = None
    return _client

# ⬇️ Tarieven per 1000 tokens


COST_PER_1K = {
    "gpt-4": float(os.getenv("COST_GPT4", 0.09)),
    "gpt-4o": float(os.getenv("COST_GPT4O", 0.02)),
    "gpt-3.5-turbo": float(os.getenv("COST_GPT35", 0.0035))
}


def analyze_with_openai(
    prompt: str,
    model: str = "gpt-4",
    temperature: float = 0.7,
    max_tokens: int = 1000,
    dry_run: bool = False
) -> dict:
    """
    Analyseert een prompt met het opgegeven OpenAI-model (v1.x).
    """
    client = _get_client()

    if dry_run or client is None:
        reason = "dry_run_enabled" if dry_run else "no_api_key"
        logging.info(
            f"[SIMULATE] Geen real call (reason={reason}) — model={model}, temp={temperature}, max_tokens={max_tokens}"
        )
        return {
            "prompt": prompt,
            "result": "[simulatie]",
            "issues": [reason],
            "model_used": model,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "tokens": {
                "prompt": 0,
                "completion": 0,
                "total": 0,
                "estimated_cost_usd": 0.0,
            },
        }

    try:
        logging.info(f"[OPENAI] Prompt verstuurd naar {model} ({len(prompt)} tekens)")

        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Je bent een behulpzame data-analist."},
                {"role": "user", "content": prompt}
            ],
            temperature=temperature,
            max_tokens=max_tokens
        )

        content = response.choices[0].message.content
        usage = response.usage

        prompt_tokens = usage.prompt_tokens
        completion_tokens = usage.completion_tokens
        total_tokens = usage.total_tokens

        cost_rate = COST_PER_1K.get(model, 0.01)
        total_cost = (total_tokens / 1000) * cost_rate

        logging.info(f"[USAGE] Prompt: {prompt_tokens}, Completion: {completion_tokens}, Total: {total_tokens}")
        logging.info(f"[COST] Geschatte kosten: ${total_cost:.4f} (model: {model}, rate: ${cost_rate}/1K tokens)")

        return {
            "result": content,
            "model_used": model,
            "tokens": {
                "prompt": prompt_tokens,
                "completion": completion_tokens,
                "total": total_tokens,
                "estimated_cost_usd": round(total_cost, 6)
            }
        }

    except Exception as e:
        logging.exception(f"[UNEXPECTED ERROR] Onbekende fout bij AI-analyse: {e}")
        return {"error": "unexpected_error", "details": str(e)}

