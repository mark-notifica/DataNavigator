import os
import logging
import requests
import yaml

# Laad modeldefinities eenmalig
def _load_model_definitions(path="model_definitions.yaml") -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f).get("models", {})

MODEL_DEFINITIONS = _load_model_definitions()

def call_model(model: str, prompt: str, temperature: float = 0.7, max_tokens: int = 1000) -> str:
    """
    Roept een LLM aan op basis van het gekozen model.
    Ondersteunt OpenAI, Azure OpenAI en self-hosted (Ollama-style).
    
    :param model: logische modelnaam, zoals "gpt-4" of "mistral-7b"
    :param prompt: string prompt
    :param temperature: float
    :param max_tokens: int
    :return: gegenereerde response tekst
    """
    if model not in MODEL_DEFINITIONS:
        raise ValueError(f"Model '{model}' is niet gedefinieerd in model_definitions.yaml")

    cfg = MODEL_DEFINITIONS[model]
    provider = cfg.get("provider")
    endpoint = cfg.get("endpoint")
    api_key = os.getenv(cfg.get("api_key_env", ""))

    if not endpoint:
        raise ValueError(f"Model '{model}' heeft geen endpoint opgegeven.")

    headers = {}
    data = {}

    if provider == "openai":
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        data = {
            "model": cfg.get("model_name", model),
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens
        }

    elif provider == "azure":
        headers = {
            "api-key": api_key,
            "Content-Type": "application/json"
        }
        deployment = cfg.get("deployment_name")
        if not deployment:
            raise ValueError(f"Azure-model '{model}' mist 'deployment_name'")
        endpoint = f"{endpoint}/openai/deployments/{deployment}/chat/completions?api-version=2024-02-15-preview"
        data = {
            "messages": [{"role": "user", "content": prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens
        }

    elif provider == "self_hosted":
        # Bijvoorbeeld Ollama-stijl
        data = {
            "model": cfg.get("model_name", model),
            "prompt": prompt,
            "temperature": temperature
        }
        headers = {"Content-Type": "application/json"}

    else:
        raise ValueError(f"Onbekende provider: {provider}")

    logging.debug(f"Verstuur verzoek naar {provider} model '{model}' via {endpoint}")

    try:
        response = requests.post(endpoint, json=data, headers=headers, timeout=60)
        response.raise_for_status()
        result = response.json()
        return _extract_response_text(provider, result)

    except Exception as e:
        logging.error(f"Fout bij modelaanroep voor {model}: {e}")
        raise

def _extract_response_text(provider: str, result: dict) -> str:
    if provider in ["openai", "azure"]:
        return result["choices"][0]["message"]["content"]
    elif provider == "self_hosted":
        return result.get("response") or result.get("text") or str(result)
    return str(result)
