from typing import Any
import dotenv
from config import MAX_LENGTH_OPTIMIZED_FILE
from scripts.checker_time import timing
from scripts.merge_files import merge_files
from scripts.worker_series import get_series
import os


def split_and_sort_files(filepath: str, pattern: str) -> tuple[str, int | Any]:
    dotenv.load_dotenv()
    base_path = os.getenv('BASE_PATH')
    filename_series = f'sorted_series'
    filepath_input = os.path.join(base_path, filename_series)
    os.makedirs(filepath_input, exist_ok=True)
    file_count = 0
    current_bytes = 0
    block_lines = []
    with open(filepath, 'r', encoding='utf-8') as f_input:
        for line in f_input:
            line_bytes = len(line.encode('utf-8'))
            if current_bytes + line_bytes > MAX_LENGTH_OPTIMIZED_FILE:
                block_lines.sort(key=lambda x: x.split(pattern)[0], reverse=True)
                file_count += 1
                output_file = os.path.join(filepath_input, f'series_{file_count}.txt')
                with open(output_file, 'w', encoding='utf-8') as f_output:
                    f_output.writelines(block_lines)
                block_lines = []
                current_bytes = 0
            block_lines.append(line)
            current_bytes += line_bytes
    if block_lines:
        block_lines.sort(key=lambda x: x.split(pattern)[0], reverse=True)
        file_count += 1
        output_file = os.path.join(filepath_input, f'series_{file_count}.txt')
        with open(output_file, 'w', encoding='utf-8') as f_output:
            f_output.writelines(block_lines)
    return filepath_input, file_count

@timing
def multipath_sort_optimized(filename: str, pattern: str) -> str:
    dir_series_path, n_files = split_and_sort_files(filename, pattern)
    dir_series_path = get_series(
        filename=filename,
        n_files=n_files,
        pattern= pattern,
    )
    return merge_files(
        path_dir_series=dir_series_path,
        pattern=pattern,
        n_files=n_files,
    )
