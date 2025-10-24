import os
import dotenv


def get_series(
        filename: str,
        n_files: int,
        pattern: str = ' - ',
) -> str:
    if n_files > 10 or n_files <= 0:
        raise ValueError('n_files must be greater than or equal to 10')
    dotenv.load_dotenv()
    base_path = os.getenv('BASE_PATH')
    input_filepath = os.path.join(base_path, filename)
    dir_filename = f'{input_filepath}_series'
    dir_series_path = os.path.join(base_path, dir_filename)
    os.makedirs(dir_series_path, exist_ok=True)
    previous_element = -float('inf')
    current_file_index = 1
    with open(input_filepath, 'r', encoding='utf-8') as f_input:
        try:
            for line in f_input:
                tokens = line.strip().split(pattern)
                key = int(tokens[0])
                if key >= previous_element:
                    current_file_index += 1
                if current_file_index > n_files:
                    current_file_index = 1
                previous_element = key
                current_filepath = os.path.join(
                    dir_series_path, f'series_{current_file_index}.txt'
                )
        except ValueError as value_error:
            print(value_error)
        except Exception as e:
            print(e)
            with open(current_filepath, 'a', encoding='utf-8') as f_output:
                f_output.write(line)
    return dir_series_path