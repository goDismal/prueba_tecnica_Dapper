import yaml
import re
import logging
from typing import Any

logger = logging.getLogger(__name__)


def load_rules(rules_path: str) -> dict:
    """
    Carga las reglas de validación desde el archivo YAML.
    """
    with open(rules_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    return config.get('fields', {})


def validate_field(value: Any, rules: dict) -> tuple[bool, Any]:
    """
    Valida un campo individual contra sus reglas.

    Returns:
        (cumple, valor)
        - cumple=True  → valor válido, se usa tal cual
        - cumple=False → valor inválido, debe quedar NULL
    """
    # Campo vacío
    if value is None or value == '':
        if rules.get('required', False):
            return False, None
        return True, None

    # Validar tipo
    expected_type = rules.get('type')
    if expected_type:
        type_map = {'str': str, 'int': int, 'bool': bool, 'float': float}
        expected = type_map.get(expected_type)
        if expected and not isinstance(value, expected):
            try:
                value = expected(value)
            except (ValueError, TypeError):
                return False, None

    # Validar longitud máxima
    max_length = rules.get('max_length')
    if max_length and isinstance(value, str) and len(value) > max_length:
        return False, None

    # Validar regex
    pattern = rules.get('regex')
    if pattern and isinstance(value, str):
        if not re.match(pattern, value):
            return False, None

    return True, value


def validate_record(record: dict, rules: dict) -> tuple[bool, dict]:
    """
    Valida un registro completo contra todas las reglas.

    Returns:
        (keep_row, validated_record)
        - keep_row=False → fila descartada completa (campo obligatorio falló)
        - keep_row=True  → fila válida (campos opcionales inválidos quedan NULL)
    """
    validated = record.copy()

    for field, field_rules in rules.items():
        value = record.get(field)
        is_valid, clean_value = validate_field(value, field_rules)

        if not is_valid:
            if field_rules.get('required', False):
                logger.debug(
                    f"Campo obligatorio '{field}' inválido (valor: '{value}'). "
                    f"Descartando fila."
                )
                return False, {}
            else:
                logger.debug(
                    f"Campo opcional '{field}' inválido (valor: '{value}'). "
                    f"Se dejará NULL."
                )
                validated[field] = None
        else:
            validated[field] = clean_value

    return True, validated


def run_validation(records: list, rules_path: str) -> list:
    """
    Punto de entrada del módulo de validación.
    Retorna lista de registros validados.
    """
    rules = load_rules(rules_path)

    total = len(records)
    validated_records = []
    discarded = 0

    logger.info(f"Iniciando validación — registros a validar: {total}")

    for record in records:
        keep, validated = validate_record(record, rules)
        if keep:
            validated_records.append(validated)
        else:
            discarded += 1

    logger.info(f"Validación completa — total: {total} | válidos: {len(validated_records)} | descartados: {discarded}")

    return validated_records