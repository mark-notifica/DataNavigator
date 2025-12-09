"""
Legacy-compatible LLM wrapper used by tests.

Exposes call_llm() and re-exports sample-data helpers so
patch targets like ai_analyzer.analysis.llm_model_wrapper.call_llm exist.
"""
from ai_analyzer.model_logic.llm_clients.openai_client import analyze_with_openai


def call_llm(prompt: str, *, model: str, temperature: float, max_tokens: int) -> dict:
    """Compatibility wrapper delegating to the OpenAI client.

    Returns a dict consistent with analyze_with_openai.
    """
    return analyze_with_openai(prompt, model=model, temperature=temperature, max_tokens=max_tokens)
