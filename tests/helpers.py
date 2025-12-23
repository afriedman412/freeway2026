from pathlib import Path
import shutil
import subprocess


EXPECTED_SUBDIRS = {"schedule_a", "schedule_e"}


def reset_data_dir(data_dir: Path):
    if not data_dir.exists():
        return

    # Only allow known subdirectories
    subdirs = {
        p.name: p
        for p in data_dir.iterdir()
        if p.is_dir()
    }

    assert set(subdirs.keys()) <= EXPECTED_SUBDIRS, (
        f"Unexpected subdirs in DATA_DIR: {set(subdirs.keys())}"
    )

    for name, d in subdirs.items():
        files = [
            p for p in d.iterdir()
            if p.is_file() and not p.name.startswith(".")
        ]

        assert len(files) == 1, (
            f"Expected exactly 1 file in {name}, found {files}"
        )

        assert files[0].suffix == ".jsonl", (
            f"Unexpected file type in {name}: {files[0]}"
        )

    # If all checks pass, delete the whole directory
    shutil.rmtree(data_dir)


def _postgres_ready(container: str) -> bool:
    result = subprocess.run(
        [
            "docker", "exec", container,
            "pg_isready",
            "-U", "postgres",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0
