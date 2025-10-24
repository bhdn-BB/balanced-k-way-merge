from scripts.checker_time import timing
from scripts.worker_series import *
from scripts.merge_files import *


@timing
def multipath_sort(
        filename: str,
        n_files: int,
        pattern: str = ' - '
) -> str:
    dir_series_path = get_series(
        filename=filename,
        n_files=n_files,
        pattern=pattern,
    )
    return merge_files(
        path_dir_series=dir_series_path,
        pattern=pattern,
        n_files=n_files
    )
