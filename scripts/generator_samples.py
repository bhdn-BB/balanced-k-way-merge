import random
from datetime import datetime, timedelta
from config import (
    LOWER_BOUND_KEY,
    UPPER_BOUND_KEY,
    LOWER_BOUND_LINE,
    UPPER_BOUND_LINE,
    UPPER_CASE_LETTER,
    LOWER_CASE_LETTER,
    LOWER_BOUND_DATE,
)


def generate_sample() -> str:
    number = random.randint(
        LOWER_BOUND_KEY,
        UPPER_BOUND_KEY
    )
    str_length = random.randint(
        LOWER_BOUND_LINE,
        UPPER_BOUND_LINE
    )
    line = ''.join(
        chr(random.choice(
            list(range(*UPPER_CASE_LETTER)) +
            list(range(*LOWER_CASE_LETTER)))
        )
        for _ in range(str_length)
    )
    start_date = datetime(*LOWER_BOUND_DATE)
    end_date = datetime.now()
    diff_date = end_date - start_date
    random_days = random.randint(0, diff_date.days)
    random_date = start_date + timedelta(days=random_days)
    formatted_date = random_date.strftime("%d/%m/%Y")
    return f"{number} - {line} - {formatted_date}"

