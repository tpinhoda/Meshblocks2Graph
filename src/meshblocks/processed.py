"""Generates processed data regarding census results"""
import os
from dataclasses import dataclass, field
from tqdm import tqdm
import geopandas as gpd
import pandas as pd
from libpysal.weights.contiguity import Queen
from libpysal.weights import DistanceBand
from src.data import Data

@dataclass
class Processed(Data):
    """Represents the Brazilian census results in processed state of processing.

    This object pre-processes the Brazilian census results.

    Attributes
    ----------
        aggregation_level: str
            The data geogrephical level of aggrevation
        na_threshold: float
            Non_NA threshold to drop column
        global_cols: int
            Whether to include global features [0, 1]
        global_threshold: float
            Threshold to remove global features
    """
    
    aggregation_level: str = None
    filename: str = None
    type_adj: str = None
    id_col: str = None
    __mesblock: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)
    
    def _get_filepath(self):
        raw_path = self._get_data_name_folders_path(state="raw")
        return  os.path.join(raw_path, 
                             self.aggregation_level, 
                             f"{self.filename.split('.')[0]}.shp")
    
    def _read_meshblock(self):
        self.__mesblock = gpd.read_file(self._get_filepath())
    
    def generate_adjacency_matrix(self):
        self.logger_info(f"Generating adjacency matrix according to: {self.aggregation_level}") 
        # Set the aggregation attribute as index
        self.__mesblock.set_index(self.id_col, inplace=True)
        # Calculate the adjacency matrix according to queen strategy
        if self.type_adj == "QUEEN":
            weights = Queen.from_dataframe(self.__mesblock)
        elif self.type_adj == "INVD":
            weights = DistanceBand.from_dataframe(self.__mesblock, threshold=None, build_sp=False, binary=False)
        else:
            self.logger_error("Wrong type_adj!")
            exit()  
        # Get the adjacency matryx
        w_matrix, _ = weights.full()
        # Associating the aggregating codes as indexes
        w_matrix = pd.DataFrame(w_matrix, index=self.__mesblock.index, columns=self.__mesblock.index)
        # Saving the adjacency matrix as csv
        w_matrix.to_csv(os.path.join(self.cur_dir, f"{self.type_adj}.csv"))


    def run(self):
        """Run processed process"""
        self.init_logger_name(msg="Meshblocks (Processed)")
        self.init_state(state="processed")
        self.logger_info("Generating processed data.")
        self._make_folders(folders=[self.data_name, self.aggregation_level])
        self._read_meshblock()
        self.generate_adjacency_matrix()
        
        
