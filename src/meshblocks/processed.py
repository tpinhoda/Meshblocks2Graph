"""Generates processed data regarding census results"""
import os
from dataclasses import dataclass, field
import geopandas as gpd
import pandas as pd
from libpysal.weights.contiguity import Queen
from libpysal.weights import DistanceBand
from src.data import NAME_MESHBLOCK, Data


@dataclass
class Processed(Data):
    """Represents the meshblocks in processed state of processing.

    This object pre-processes meshblocks data.

    Attributes
    ----------
        aggregation_level: str
            The data geogrephical level of aggrevation
        filename: str
            The shp filename
        type_adj: str
            The type of graph (QUEEN or INVD)
        id_col: str
            The id column
        rename_id_col: str
            The new name of the id column
    """

    aggregation_level: str = None
    filename: str = None
    type_adj: str = None
    id_col: str = None
    renamed_id_col: str = None
    __mesblock: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)

    def _get_filepath(self):
        """Return the meshblocks path in the raw state"""
        raw_path = self._get_data_name_folders_path(state="raw")
        return os.path.join(
            raw_path, self.aggregation_level,NAME_MESHBLOCK
        )

    def _read_meshblock(self):
        """Read meshblock file"""
        self.__mesblock = gpd.read_file(self._get_filepath())

    def _rename_id_col(self):
        """Rename the id column"""
        self.__mesblock.rename(columns={self.id_col: self.renamed_id_col}, inplace=True)

    def generate_adjacency_matrix(self):
        """Generates the adjacency matrix"""
        self.logger_info(
            f"Generating adjacency matrix according to: {self.aggregation_level}"
        )
        # Set the aggregation attribute as index
        self.__mesblock.set_index(self.renamed_id_col, inplace=True)
        # Calculate the adjacency matrix according to queen strategy
        if self.type_adj == "QUEEN":
            weights = Queen.from_dataframe(self.__mesblock)
        elif self.type_adj == "INVD":
            weights = DistanceBand.from_dataframe(
                self.__mesblock, threshold=None, build_sp=False, binary=False
            )
        else:
            self.logger_error("Wrong type_adj!")
            exit()
        # Get the adjacency matryx
        w_matrix, _ = weights.full()
        # Associating the aggregating codes as indexes
        w_matrix = pd.DataFrame(
            w_matrix, index=self.__mesblock.index, columns=self.__mesblock.index
        )
        # Saving the adjacency matrix as csv
        w_matrix.to_csv(os.path.join(self.cur_dir, f"{self.type_adj}.csv"))

    def run(self):
        """Run processed process"""
        self.init_logger_name(msg="Meshblocks (Processed)")
        self.init_state(state="processed")
        self.logger_info("Generating processed data.")
        self._make_folders(folders=[self.data_name, self.aggregation_level])
        self._read_meshblock()
        self._rename_id_col()
        self.generate_adjacency_matrix()
