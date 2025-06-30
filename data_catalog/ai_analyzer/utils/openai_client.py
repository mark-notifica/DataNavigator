import os
import logging
from dotenv import load_dotenv

try:
    import openai
except ImportError:
    raise ImportError(
        "Het 'openai' pakket is niet geïnstalleerd. "
        "Voer 'pip install openai' uit in je virtuele omgeving."
    )

# ⬇️ Laad .env (als die er is)
load_dotenv()

# ⬇️ Instellingen uit omgeving
openai.api_key = os.getenv("OPENAI_API_KEY")
MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4")
MAX_TOKENS = int(os.getenv("OPENAI_MAX_TOKENS", 800))
TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", 0.2))

if not openai.api_key:
    raise RuntimeError("❌ Geen OPENAI_API_KEY gevonden. Zet die in je .env of als omgevingsvariabele.")


def analyze_with_openai(prompt: str, dry_run: bool = False) -> dict:
    if dry_run:
        logging.info("[DRY RUN] OpenAI-analyse gesimuleerd. Prompt niet verstuurd.")
        return {
            "prompt": prompt,
            "result": "[simulatie]",
            "issues": ["dry_run_enabled"]
        }

    try:
        logging.info(f"[OPENAI] Prompt verstuurd naar {MODEL_NAME} ({len(prompt)} tekens)")

        response = openai.ChatCompletion.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": "Je bent een behulpzame data-analist."},
                {"role": "user", "content": prompt}
            ],
            temperature=TEMPERATURE,
            max_tokens=MAX_TOKENS
        )

        content = response.choices[0].message["content"]
        usage = response.get("usage", {})

        prompt_tokens = usage.get("prompt_tokens", 0)
        completion_tokens = usage.get("completion_tokens", 0)
        total_tokens = usage.get("total_tokens", 0)

        # Kosten (USD per 1000 tokens) per model
        cost_per_1k = {
            "gpt-4": 0.03 + 0.06,
            "gpt-4o": 0.005 + 0.015,
            "gpt-3.5-turbo": 0.0015 + 0.002
        }
        cost_rate = cost_per_1k.get(MODEL_NAME, 0.01)
        total_cost = (total_tokens / 1000) * cost_rate

        logging.info(f"[USAGE] Prompt: {prompt_tokens}, Completion: {completion_tokens}, Total: {total_tokens}")
        logging.info(f"[COST] Geschatte kosten: ${total_cost:.4f} (model: {MODEL_NAME})")

        return {
            "prompt": prompt,
            "result": content,
            "tokens": {
                "prompt": prompt_tokens,
                "completion": completion_tokens,
                "total": total_tokens,
                "estimated_cost_usd": round(total_cost, 6)
            }
        }

    except openai.error.AuthenticationError as e:
        logging.error(f"[AUTH ERROR] Ongeldige API key of geen toegang: {e}")
        return {"error": "authentication_error", "details": str(e)}

    except openai.error.OpenAIError as e:
        logging.error(f"[OPENAI ERROR] Fout bij OpenAI-aanroep: {e}")
        return {"error": "openai_error", "details": str(e)}

    except Exception as e:
        logging.exception(f"[UNEXPECTED ERROR] Onbekende fout bij AI-analyse: {e}")
        return {"error": "unexpected_error", "details": str(e)}
