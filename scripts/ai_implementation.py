import os
import io
import math
import heapq
import uuid
import dotenv
import logging
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from scripts.checker_time import timing

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------- Configuration constants ----------
DEFAULT_MEMORY_LIMIT_MB = 500  # total memory budget for the merge process
# Conservative per-stream buffer allocated when a stream is opened (bytes)
DEFAULT_PER_STREAM_BUFFER = 4 * 1024 * 1024  # 4 MB per stream buffer
# Reserved memory for overhead (heap objects, Python overhead, other buffers)
DEFAULT_RESERVED_OVERHEAD = 64 * 1024 * 1024  # 64 MB reserved
# Writer buffer
DEFAULT_WRITER_BUFFER = 4 * 1024 * 1024  # 4 MB writer buffer
# Minimal k (must merge at least 2-way)
MIN_K = 2
# Default maximum allowed files to create in get_series (your code had a check)
MAX_N_FILES = 10
# --------------------------------------------

def parse_key_from_line(line: str, pattern: str = ' - ') -> Optional[int]:
    """
    Parse integer key from a line with the `pattern` separator.
    Returns None if parsing fails.
    """
    if not line:
        return None
    parts = line.strip().split(pattern, 1)
    if not parts or not parts[0]:
        return None
    try:
        return int(parts[0])
    except ValueError:
        return None


def choose_k(n_files: int,
             memory_limit_bytes: int = DEFAULT_MEMORY_LIMIT_MB * 1024 * 1024,
             per_stream_buffer: int = DEFAULT_PER_STREAM_BUFFER,
             reserved_for_overhead: int = DEFAULT_RESERVED_OVERHEAD,
             writer_buffer: int = DEFAULT_WRITER_BUFFER) -> int:
    """
    Choose how many files to merge at once (k) based on memory constraints.
    Conservative calculation:
      memory needed per merge worker ~= k * per_stream_buffer + writer_buffer + reserved_for_overhead
    We then solve for k so this is <= memory_limit_bytes.
    """
    if n_files <= 1:
        return 1
    usable = memory_limit_bytes - reserved_for_overhead - writer_buffer
    if usable <= per_stream_buffer:
        # Can't even fit one stream's buffer after overhead; fall back to 2-way merge
        return MIN_K if n_files >= MIN_K else n_files
    max_k = usable // per_stream_buffer
    # bound max_k to reasonable values
    k = max(MIN_K, min(n_files, int(max_k)))
    # ensure we don't return 1 unless n_files == 1
    return max(k, MIN_K) if n_files >= MIN_K else n_files


def chunked(iterable: List[str], size: int) -> List[List[str]]:
    """Split list into chunks of given size (last chunk may be smaller)."""
    return [iterable[i:i + size] for i in range(0, len(iterable), size)]


def merge_k_files(input_paths: List[str],
                  output_path: str,
                  pattern: str = ' - ',
                  buffer_size: int = DEFAULT_PER_STREAM_BUFFER):
    """
    Merge k sorted input files into a single output file.
    Assumptions:
      - Each input file is sorted in DESCENDING order by integer key.
      - We perform a max-heap merge using heapq on negated keys.
    Memory usage:
      - Only one line per input stream is kept in memory (plus heap overhead).
      - BufferedReader/Writer uses buffer_size bytes per stream.
    """
    if not input_paths:
        raise ValueError("input_paths must be a non-empty list.")

    # Open all input streams as buffered readers
    readers = []
    try:
        for p in input_paths:
            f = open(p, 'r', encoding='utf-8', newline='')
            br = io.BufferedReader(f, buffer_size=buffer_size)
            readers.append((p, f, br))
    except Exception:
        # cleanup if any open failed
        for _, f, br in readers:
            try:
                br.close()
                f.close()
            except Exception:
                pass
        raise

    # Prepare output buffered writer
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    out_f = open(output_path, 'w', encoding='utf-8', newline='')
    out_writer = io.BufferedWriter(out_f, buffer_size=buffer_size)

    # heap entries: (neg_key, tie_breaker_uuid, line_bytes, reader_index)
    # Use uuid/tie-breaker to avoid comparison issues when keys equal.
    heap = []
    try:
        for idx, (p, f, br) in enumerate(readers):
            # Read first non-empty line from each stream
            # We must use f.readline() (text) to preserve correct splitting and parsing.
            # But to respect buffer, read via the file object; BufferedReader.read() returns bytes.
            # Simpler: use the original text file handle .readline() to get str.
            line = f.readline()
            while line != '' and line.strip() == '':
                line = f.readline()
            if line == '':
                # empty file, skip
                logger.debug("Input file '%s' is empty; skipping.", p)
                continue
            key = parse_key_from_line(line, pattern)
            if key is None:
                logger.warning("Could not parse key from first line of '%s'; skipping that line", p)
                # try next lines
                while True:
                    line = f.readline()
                    if line == '':
                        break
                    if line.strip() == '':
                        continue
                    key = parse_key_from_line(line, pattern)
                    if key is not None:
                        break
                if line == '' or key is None:
                    logger.warning("No valid lines left in '%s'; file will be ignored.", p)
                    continue
            # We want DESCENDING order -> use negated key in a min-heap
            tie = uuid.uuid4().hex
            # store the original string including newline char as it was read
            heapq.heappush(heap, (-key, tie, line, idx))
        # If heap empty -> nothing to write
        if not heap:
            logger.info("No data available from inputs; creating empty output: %s", output_path)
            out_writer.flush()
            out_writer.close()
            out_f.close()
            return

        # We will keep using the opened text file handles for subsequent reads
        while heap:
            neg_key, tie, line, reader_idx = heapq.heappop(heap)
            # Write the popped line to output
            # write expects bytes for BufferedWriter
            out_writer.write(line.encode('utf-8'))
            # Now pull next line from the same reader
            p, f, br = readers[reader_idx]
            next_line = f.readline()
            while next_line != '' and next_line.strip() == '':
                next_line = f.readline()
            if next_line == '':
                # this reader is exhausted
                continue
            next_key = parse_key_from_line(next_line, pattern)
            if next_key is None:
                # skip malformed lines until a valid key or EOF
                while True:
                    next_line = f.readline()
                    if next_line == '':
                        break
                    if next_line.strip() == '':
                        continue
                    next_key = parse_key_from_line(next_line, pattern)
                    if next_key is not None:
                        break
                if next_line == '' or next_key is None:
                    # exhausted or no valid lines
                    continue
            tie = uuid.uuid4().hex
            heapq.heappush(heap, (-next_key, tie, next_line, reader_idx))
    finally:
        try:
            out_writer.flush()
            out_writer.close()
            out_f.close()
        except Exception:
            pass
        # Close all readers
        for _, f, br in readers:
            try:
                br.close()
            except Exception:
                pass
            try:
                f.close()
            except Exception:
                pass


def merge_files(path_dir_series: str,
                pattern: str,
                n_files: int,
                memory_limit_mb: int = DEFAULT_MEMORY_LIMIT_MB,
                per_stream_buffer: int = DEFAULT_PER_STREAM_BUFFER,
                reserved_overhead: int = DEFAULT_RESERVED_OVERHEAD,
                writer_buffer: int = DEFAULT_WRITER_BUFFER,
                parallel_jobs: Optional[int] = None) -> str:
    """
    Iteratively merge files in `path_dir_series` until a single sorted file is produced.
    - path_dir_series: directory containing the initial run files (series_*.txt)
    - pattern: separator used to parse integer key from lines
    - n_files: original n_files parameter (used for validation)
    - memory_limit_mb: total memory budget (MB) for the entire merge process
    - per_stream_buffer / reserved_overhead / writer_buffer: tune memory usage
    - parallel_jobs: optionally allow merging multiple groups in parallel; if None, compute conservative bound.
    Returns path to final merged file (result.txt in BASE_PATH).
    """
    dotenv.load_dotenv()
    base_path = os.getenv('BASE_PATH') or os.getcwd()

    # Collect initial file paths (only files, ignore directories)
    file_paths = [os.path.join(path_dir_series, f) for f in os.listdir(path_dir_series)
                  if os.path.isfile(os.path.join(path_dir_series, f))]
    file_paths.sort()  # deterministic order

    if not file_paths:
        raise ValueError("No series files found in the directory: {}".format(path_dir_series))

    memory_limit_bytes = int(memory_limit_mb * 1024 * 1024)
    total_files = len(file_paths)

    # Compute k based on memory budget
    k = choose_k(
        n_files=total_files,
        memory_limit_bytes=memory_limit_bytes,
        per_stream_buffer=per_stream_buffer,
        reserved_for_overhead=reserved_overhead,
        writer_buffer=writer_buffer
    )
    logger.info("Merging with k=%d (memory limit %d MB, %d input files).",
                k, memory_limit_mb, total_files)

    # Compute safe parallelism:
    # each parallel merge worker will use approx: k * per_stream_buffer + writer_buffer + reserved_overhead
    per_worker_bytes = k * per_stream_buffer + writer_buffer + reserved_overhead
    if per_worker_bytes <= 0:
        max_workers = 1
    else:
        max_workers = max(1, memory_limit_bytes // per_worker_bytes)
    # Bound max_workers by CPU count and user-specified parallel_jobs
    cpu_bound = os.cpu_count() or 1
    computed_workers = min(max_workers, cpu_bound)
    if parallel_jobs is None:
        parallel_jobs = computed_workers
    else:
        parallel_jobs = max(1, min(parallel_jobs, computed_workers))

    logger.info("Parallel merge groups limit: %d workers (computed %d, cpu %d).",
                parallel_jobs, computed_workers, cpu_bound)

    pass_index = 0
    current_paths = file_paths
    # iterative merging passes
    while len(current_paths) > 1:
        pass_index += 1
        logger.info("Starting merge pass %d: %d files -> grouping by k=%d", pass_index, len(current_paths), k)
        merged_dirname = os.path.join(base_path, f'merged_pass_{pass_index}')
        os.makedirs(merged_dirname, exist_ok=True)

        groups = chunked(current_paths, k)
        merged_output_paths = []

        # Helper to create a unique name for merged group
        def merged_output_for_group(gidx: int) -> str:
            return os.path.join(merged_dirname, f'merged_pass{pass_index}_group{gidx}.txt')

        # Prepare tasks
        tasks = []
        for gi, group in enumerate(groups, start=1):
            outp = merged_output_for_group(gi)
            merged_output_paths.append(outp)
            tasks.append((group, outp))

        # Execute merges (possibly parallel)
        if parallel_jobs > 1 and len(tasks) > 1:
            with ThreadPoolExecutor(max_workers=parallel_jobs) as executor:
                future_to_task = {executor.submit(merge_k_files, group, outp, pattern, per_stream_buffer): (group, outp)
                                  for group, outp in tasks}
                for fut in as_completed(future_to_task):
                    group, outp = future_to_task[fut]
                    try:
                        fut.result()
                        logger.info("Merged %d files -> %s", len(group), outp)
                    except Exception as e:
                        logger.exception("Merging group with output %s failed: %s", outp, e)
                        raise
        else:
            # Sequential execution
            for group, outp in tasks:
                merge_k_files(group, outp, pattern, per_stream_buffer)
                logger.info("Merged %d files -> %s", len(group), outp)

        # After this pass, set current_paths to newly created merged files
        current_paths = [p for p in merged_output_paths if os.path.exists(p)]
        # If somehow none created, fail
        if not current_paths:
            raise RuntimeError("Merging passes produced no output files. Aborting.")

    # Final single file available at current_paths[0]
    final_merged = current_paths[0]
    result_path = os.path.join(base_path, 'result.txt')
    # Move or rename to canonical result path (overwrite if exists)
    try:
        if os.path.exists(result_path):
            os.remove(result_path)
        os.replace(final_merged, result_path)
    except Exception:
        # fallback: copy
        with open(final_merged, 'rb') as src, open(result_path, 'wb') as dst:
            while True:
                chunk = src.read(8192)
                if not chunk:
                    break
                dst.write(chunk)
        os.remove(final_merged)
    logger.info("Final merged file created at: %s", result_path)
    return result_path


# ----------------- Existing helper (rewritten slightly) -----------------
def get_series(
        filename: str,
        n_files: int,
        pattern: str = ' - ') -> str:
    """
    Your original get_series implementation (slightly hardened).
    It produces a dir with series_*.txt files (initial runs).
    """
    if n_files > MAX_N_FILES or n_files <= 0:
        raise ValueError(f'n_files must be between 1 and {MAX_N_FILES}')

    dotenv.load_dotenv()
    base_path = os.getenv('BASE_PATH') or os.getcwd()
    input_filepath = os.path.join(base_path, filename)
    if not os.path.exists(input_filepath):
        raise FileNotFoundError(f"Input file not found: {input_filepath}")

    dir_filename = f'{os.path.basename(input_filepath)}_series'
    dir_series_path = os.path.join(base_path, dir_filename)
    os.makedirs(dir_series_path, exist_ok=True)

    previous_element = -float('inf')
    current_file_index = 1

    with open(input_filepath, 'r', encoding='utf-8') as f_input:
        for line in f_input:
            if not line or line.strip() == '':
                continue
            tokens = line.rstrip('\n').split(pattern, 1)
            try:
                key = int(tokens[0])
            except Exception:
                # skip lines with invalid keys
                logger.warning("Skip line with invalid key: %s", line.strip())
                continue

            # natural runs logic (your original idea)
            if key >= previous_element:
                current_file_index += 1
            if current_file_index > n_files:
                current_file_index = 1
            previous_element = key
            current_filepath = os.path.join(dir_series_path, f'series_{current_file_index}.txt')
            with open(current_filepath, 'a', encoding='utf-8') as f_output:
                f_output.write(line)
    return dir_series_path

@timing
def multipath_sort_ai(filename: str, n_files: int) -> str:
    """
    Top-level function that runs get_series then merge_files.
    Returns path to final sorted file.
    """
    dir_series_path = get_series(filename=filename, n_files=n_files, pattern=' - ')
    result = merge_files(
        path_dir_series=dir_series_path,
        pattern=' - ',
        n_files=n_files
    )
    return result
