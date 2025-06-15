import tarfile
from utils.data_download.ghcn import DEFAULT_DATA_DIR, COMPRESSED_DATA_FILE


def list_tar_contents():
    """List all contents of the tar.gz file in the data directory."""
    tar_path = DEFAULT_DATA_DIR / COMPRESSED_DATA_FILE
    if not tar_path.exists():
        raise FileNotFoundError(f"Tar file not found at {tar_path}")

    with tarfile.open(tar_path, "r:gz") as tar:
        return tar.getnames()


if __name__ == "__main__":
    try:
        contents = list_tar_contents()
        print(f"Contents of {COMPRESSED_DATA_FILE}:")
        for item in contents:
            print(f" - {item}")
    except Exception as e:
        print(f"Error listing tar contents: {e}")
