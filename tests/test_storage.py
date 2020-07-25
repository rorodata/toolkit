import pytest
import shutil
import pandas as pd
import requests
from pathlib import Path
from toolkit import StoragePath
from toolkit.storage import _get_aws_settings

aws_test_settings = _get_aws_settings(
    "TEST_AWS_ACCESS_KEY_ID",
    "TEST_AWS_SECRET_ACCESS_KEY",
    "TEST_AWS_ENDPOINT_URL",
    "TEST_AWS_DEFAULT_REGION"
)

@pytest.fixture
def storage_root():
    root =  StoragePath("test", "storage", aws_settings=aws_test_settings)
    root.rmdir()   # Make sure that storage root is clean if exists
    return root

class TestStoragePath:
    def test_file_exists(self, storage_root):
        test_file = storage_root / "test_file.txt"
        assert test_file.exists() == False
        test_file.write_text("Hello, World!")
        assert test_file.exists() == True

    def test_remove_file(self, storage_root):
        test_file = storage_root / "test_file.txt"
        test_file.write_text("Hello, World!")
        assert test_file.exists() == True
        test_file.unlink()
        assert test_file.exists() == False

    def test_remove_directory(self, storage_root):
        dir_path = storage_root / "dir"
        test_file1 = storage_root / "dir" / "test_file1.txt"
        test_file2 = storage_root / "dir" / "test_file2.txt"
        test_file1.write_text("Hello, World!")
        test_file2.write_text("Hello, World!")

        assert dir_path.dir_exists() == True
        dir_path.rmdir()
        assert dir_path.dir_exists() == False

    def test_download(self, storage_root):
        file_content = "Hello, World!"
        download_loc = "/tmp/test_file.txt"

        storage_path = storage_root / "test_file.txt"
        storage_path.write_text(file_content)

        storage_path.download(download_loc)
        assert open(download_loc).read() == file_content

    def test_upload(self, storage_root):
        file_content = "Hello, World!"
        local_path = "/tmp/upload.txt"
        open(local_path, "w").write(file_content)

        storage_path = storage_root / "upload.txt"
        storage_path.upload(local_path)
        assert storage_path.read_text() == file_content

    def test_read(self, storage_root):
        file_content = "Hello, World!"
        storage_path = storage_root / "test_file.txt"
        storage_path.write_text(file_content)

        assert storage_path.read_text() == file_content
        assert storage_path.read_bytes() == bytes(file_content, "utf-8")

    def test_read_write_csv(self, storage_root):
        df = pd.DataFrame({"a": [10], "b": [20]})
        storage_path = storage_root / "test_file.csv"
        storage_path.write_csv(df)
        storage_df = storage_path.read_csv()
        assert storage_df.equals(df)

    def test_read_write_parquet(self, storage_root):
        df = pd.DataFrame({"a": [10], "b": [20]})
        storage_path = storage_root / "test_file.parq"
        storage_path.write_parquet(df)
        storage_df = storage_path.read_parquet()
        assert storage_df.equals(df)

    def test_iterdir(self, storage_root):
        file_content = "Hello, World!"
        storage_path = storage_root / "test_file.txt"
        storage_path.write_text(file_content)

        storage_files = [each.path for each in storage_root.iterdir()]
        assert storage_files == [storage_path.path]

    def test_sync_to(self, storage_root):
        """test sync to localpath.
        """
        file_content = "Hello, World!"
        local_root = "/tmp/root"
        storage_path = storage_root / "test_file.txt"
        storage_path.write_text(file_content)

        storage_root.sync_to(local_root)
        assert [each.name for each in Path(local_root).glob("*")] == ["test_file.txt"]

    def test_sync_from(self, storage_root):
        """Test sync from localpath.
        """
        file_content = "Hello, World!"
        local_root = "/tmp/root"

        root = Path(local_root)
        if root.exists():
            shutil.rmtree(local_root)
        root.mkdir()
        open(root / "test_file.txt", "w").write(file_content)

        storage_root.sync_from(local_root)
        storage_files = [each.name for each in storage_root.iterdir()]
        assert storage_files == ["test_file.txt"]

    def test_copy(self, storage_root):
        """Copy from one storage path to other.
        """
        file_content = "Hello, World!"
        source_path = storage_root / "orig.txt"
        dest_path = storage_root / "copy.txt"
        source_path.write_text(file_content)

        dest_path.copy(source_path)
        assert dest_path.read_text() == file_content

    def test_copy_dir(self, storage_root):
        """Copy from one storage path to other.
        """
        file_content = "Hello, World!"
        source_dir = storage_root / "orig"
        source_path =  source_dir /"orig.txt"

        dest_dir = storage_root / "copy"
        dest_path = dest_dir / "orig.txt"
        source_path.write_text(file_content)

        assert dest_path.exists() == False
        dest_dir.copy_dir(source_dir)
        assert dest_path.exists() == True
        assert dest_path.read_text() == file_content

    def test_move(self, storage_root):
        """move from one storage path to other.
        """
        file_content = "Hello, World!"
        source_path = storage_root / "orig.txt"
        dest_path = storage_root / "copy.txt"
        source_path.write_text(file_content)

        dest_path.move(source_path)
        assert dest_path.read_text() == file_content
        assert source_path.exists() == False

    def test_move_dir(self, storage_root):
        """move from one storage dir to other.
        """
        file_content = "Hello, World!"
        source_dir = storage_root / "orig"
        source_path =  source_dir /"orig.txt"

        dest_dir = storage_root / "copy"
        dest_path = dest_dir / "orig.txt"
        source_path.write_text(file_content)

        assert dest_path.exists() == False
        dest_dir.move_dir(source_dir)
        assert dest_path.exists() == True
        assert dest_path.read_text() == file_content
        assert source_dir.dir_exists() == False

    def test_presigned_url_for_download(self, storage_root):
        file_content = "Hello, World!"
        source_file = storage_root / "test_file.txt"
        source_file.write_text(file_content)

        url = source_file.get_presigned_url_for_download()
        assert requests.get(url).text == file_content

    def test_presigned_url_for_upload(self, storage_root):
        file_content = "Hello, World!"
        dest_file = storage_root / "test_file.txt"
        assert dest_file.exists() == False

        url = dest_file.get_presigned_url_for_upload(content_type="text/html")
        requests.put(url, data=file_content, headers={"Content-type": "text/html"})
        assert dest_file.exists() == True
        assert dest_file.read_text() == file_content
