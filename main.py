import argparse
from scripts.generator_files import generate_file
from scripts.multipath_sort import multipath_sort
from scripts.optimized_multipath_sort import multipath_sort_optimized
from scripts.ai_implementation import multipath_sort_ai


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Test Balanced Multiway Merge Sort"
    )
    parser.add_argument(
        "--filepath",
        type=str,
        required=True,
        help="the file path for sorting",
    )
    parser.add_argument(
        "--n_files",
        type=int,
        default=3,
        help="Number of target merged files if it isn't optimized",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        default=" - ",
        help="File pattern to match",
    )
    parser.add_argument(
        "--size",
        type=int,
        required=True,
        help="Size of file for sorting",
    )
    args = parser.parse_args()
    generate_file(filename=args.filepath, filesize_mb=args.size)
    print('Start multipath_sort sorting')
    multipath_sort(filename=args.filepath, n_files=args.n_files, pattern=args.pattern)
    print('Start multipath_sort_optimized sorting')
    multipath_sort_optimized(filename=args.filepath, pattern=args.pattern)
    print('Start multipath_sort_ai sorting')
    multipath_sort_ai(filename=args.filepath, pattern=args.pattern)
