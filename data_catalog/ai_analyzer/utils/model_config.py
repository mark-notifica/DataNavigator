import logging
from ai_analyzer.analysis.analysis_matrix import ANALYSIS_TYPES


def get_model_config(analysis_type: str, ai_config: dict) -> tuple[str, float, int, str]:
    """
    Bepaalt welk model, temperatuur en max_tokens gebruikt moeten worden voor een analyse.
    ANALYSIS_TYPES is leidend; ai_config mag overrulen.
    Geeft ook de bron van de instellingen terug (analysis_type_default, ai_config of fallback).

    :param analysis_type: het type analyse (bijv. 'column_classification')
    :param ai_config: dictionary met mogelijke overrides
    :return: tuple (model, temperature, max_tokens, model_config_source)
    """
    atype_config = ANALYSIS_TYPES.get(analysis_type, {})

    model = atype_config.get("default_model", "gpt-4")
    temperature = atype_config.get("temperature", 0.7)
    max_tokens = atype_config.get("max_tokens", 1000)
    model_config_source = "analysis_type_default"

    # AI-config overschrijft indien aanwezig
    if ai_config:
        overridden = False

        if ai_config.get("model") and ai_config["model"] != model:
            logging.info(f"[OVERRIDE] Model overschreven door ai_config: {ai_config['model']} (was: {model})")
            model = ai_config["model"]
            overridden = True

        if ai_config.get("temperature") is not None and ai_config["temperature"] != temperature:
            logging.info(f"[OVERRIDE] Temperature overschreven door ai_config: {ai_config['temperature']} (was: {temperature})")
            temperature = ai_config["temperature"]
            overridden = True

        if ai_config.get("max_tokens") is not None and ai_config["max_tokens"] != max_tokens:
            logging.info(f"[OVERRIDE] Max tokens overschreven door ai_config: {ai_config['max_tokens']} (was: {max_tokens})")
            max_tokens = ai_config["max_tokens"]
            overridden = True

        if overridden:
            model_config_source = "ai_config"

    # Fallback als alles ontbreekt
    if not model:
        model = "gpt-4"
        model_config_source = "fallback"

    return model, temperature, max_tokens, model_config_source
