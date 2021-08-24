"""Generate raw data for census"""
import os
import zipfile
from dataclasses import dataclass
from urllib.request import urlretrieve
from src.data import Data


@dataclass
class Raw(Data):
    """Represents the meshblock data in raw processing state.

    This object downloads meshblock data.

    Attributes
    ----------
        url: str
            The url to collect the raw data
        aggregation_level: str
            The geographical aggregation level
        filename: str
            The donwloaded filename

    """

    url_data: str = None
    aggregation_level: str = None
    filename: str = None

    # Get meshblock files
    def _download_city_meshblock_data(self) -> None:
        """Donwload raw election data"""
        self.logger_info("Downloading city meshblock data.")
        urlretrieve(self.url_data, os.path.join(self.cur_dir, self.filename))

    def _unzip_city_meshblock_data(self) -> None:
        """Unzip only the csv raw data in the current directory"""
        self.logger_info("Unzipping city meshblock file.")
        with zipfile.ZipFile(os.path.join(self.cur_dir, self.filename), "r") as zip_ref:
            zip_ref.extractall(self.cur_dir)

    def _rename_meshblock_files(self):
        """Rename cities meshblocks files"""
        self.logger_info("Renaming zip files.")
        list_filename = self._get_files_in_cur_dir()
        for filename in list_filename:
            old_name = filename
            new_name = f"{self.filename.split('.')[0]}.{filename.split('.')[1]}"
            self._rename_file_from_cur_dir(old_name, new_name)

    def _remove_city_meshblock_zip_files(self) -> None:
        """Remove all zip files in the current directory"""
        self.logger_info("Removing zip files.")
        list_filename = self._get_files_in_cur_dir()
        for filename in list_filename:
            if filename.endswith(".zip"):
                self._remove_file_from_cur_dir(filename=filename)

    def _empty_folder_run(self):
        """Donwload and unzip cities meshblocks"""
        self._download_city_meshblock_data()
        self._unzip_city_meshblock_data()
        self._rename_meshblock_files()
        self._remove_city_meshblock_zip_files()

    def run(self) -> None:
        """Generate meshblocks raw data"""
        self.init_logger_name(msg="Meshblocks (Raw)")
        self.init_state(state="raw")
        self.logger_info("Generating raw data.")
        self._make_folders(folders=[self.data_name, self.aggregation_level])
        files_exist = self._get_files_in_cur_dir()
        if not files_exist:
            self._empty_folder_run()
        else:
            self.logger_warning(
                "Non empty directory, the process only runs on empty folders!"
            )
