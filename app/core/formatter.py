import re

def format_step_response(text: str) -> str:
    # Check for list indicators
    indicators = [
        r'^\d+\.',       # 1. 
        r'(?i)step \d+', # Step 1
        r'(?i)first,',   # First,
        r'(?i)next,',    # Next,
        r'(?i)then,'     # Then,
        r'-'           # Bullet point
    ]

    is_procedure = any(re.search(ind, text, re.MULTILINE) for ind in indicators)

    if is_procedure:
        # Normalize newlines
        lines = text.replace('. ', '.').split('\n')
        formatted_lines = []
        counter = 1

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # If it already starts with number, keep it
            if re.match(r'^\d+\.', line):
                formatted_lines.append(line)
            # If it's a bullet, convert to number
            elif line.startswith('- ') or line.startswith('* '):
                 formatted_lines.append(f"{counter}. {line[2:]}")
                 counter += 1
            else:
                 formatted_lines.append(line)

        return "\n".join(formatted_lines)

    return text