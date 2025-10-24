import os
import dotenv


def merge_files(
        path_dir_series: str,
        pattern: str,
        n_files: int
) -> str:

    dotenv.load_dotenv()
    base_path = os.getenv('BASE_PATH')

    file_paths = [
        os.path.join(base_path, f) for f in os.listdir(path_dir_series)
    ]

    index = 1
    k = 0
    filepath_output = ''

    while k != 1:
        streams = [open(f, 'r') for f in file_paths]
        current_merged_dirname = f'merged_{index}_file{index+1}'
        current_file_path = os.path.join(base_path, current_merged_dirname)
        os.makedirs(current_file_path, exist_ok=True)
        k = 0
        for idx, stream in enumerate(streams):
            lines = [s.strip() for s in stream]
            lines.sort(key=lambda a: a.split(pattern)[0])
            lines = lines[::-1]
            idx = idx % n_files
            k = k + 1 if k < 3 else k
            filename =f'merged#{index}_file#{idx}.txt'
            filepath_output = os.path.join(current_merged_dirname, filename)
            f_current = open(filepath_output, 'a', encoding='utf-8')
            f_current.writelines(lines)
            f_current.close()
        for s in streams:
            s.close()
        index += 1
        file_paths = [
            os.path.join(base_path, f) for f in os.listdir(current_file_path)
        ]
    result_path = os.path.join(base_path, 'result.txt')
    os.rename(filepath_output, result_path)
    return result_path