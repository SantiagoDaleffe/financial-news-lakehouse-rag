import re

PII_PATTERNS = {
    "CREDIT_CARD": r"\b(?:\d[ -]*?){13,16}\b",
    "EMAIL": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b",
    "PHONE_NUMBER": r"\b(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\b",
    "DNI": r"\b\d{1,2}\.?\d{3}\.?\d{3}\b",
}


def mask_pii(text: str) -> str:
    """Mask personally identifiable information in a given text.

    Args:
        text (str): The input string that may contain PII values.

    Returns:
        str: The text with detected PII values replaced by redaction markers.
    """
    if not text:
        return text

    masked_text = text
    for pii_type, pattern in PII_PATTERNS.items(): 
        masked_text = re.sub(pattern, f"[{pii_type}_REDACTED]", masked_text)

    return masked_text
