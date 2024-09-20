from decimal import Decimal # Import Decimal to convert floats to Decimals for DynamoDB

def parse_address(unparsed_address):
    """
    Removes non-alphanumeric characters from the unparsed address and replaces spaces with '+' for URL encoding.
    Args: unparsed_address (str): The raw address string to be parsed.
    Returns: str: The parsed address suitable for API usage.
    """
    parsed_address = ''
    for char in unparsed_address:
        if not char.isalnum():
            if char == ' ':
                parsed_address += '+'
        else:
            parsed_address += char
    return parsed_address


# Convert floats to Decimal for DynamoDB
def convert_floats_to_decimal(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))  # Convert float to Decimal
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}  # Recursively process dictionary
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(i) for i in obj]  # Recursively process list
    else:
        return obj  # Return the object as is if it's neither a float, dict, nor list