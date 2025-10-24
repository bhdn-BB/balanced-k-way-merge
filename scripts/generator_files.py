from config import CONVERT_TO_MB
from .generator_samples import generate_sample
from tqdm import tqdm


def generate_file(filename: str, filesize_mb: int) -> None:
    current_size = 0
    target_size = filesize_mb * CONVERT_TO_MB
    with (open(filename, "w", encoding="utf-8") as f,
          tqdm(
              total=target_size,
              unit='B',
              unit_scale=True,
              desc=filename) as pbar
          ):
        while current_size < target_size:
            line = generate_sample() + "\n"
            f.write(line)
            line_size = len(line.encode('utf-8'))
            current_size += line_size
            pbar.update(line_size)