"""Pipeline to process meshblock data and generate spatial graph structure"""
from dataclasses import dataclass, field
from typing import Dict, Final, List
import inspect
from src.data import Data
from src.meshblocks.raw import Raw as MeshblocksRaw
from src.meshblocks.processed import Processed as MeshblocksProcessed

DATA_PROCESS_MAP: Final = {
    "meshblocks": {
        "raw": MeshblocksRaw,
        "processed": MeshblocksProcessed,
    },
}


@dataclass
class Pipeline:
    """Pipeline to process meshblock data and generate spatial graph structure.

    This object process meshblock data and generates spatial graph.

    Attributes
    ----------
    data_name: str
        Describes the type of data [location or results]
    params: Dict[str, str]
        Dictionary of parameters
    switchers: Dict[str, int]
        Dictionary of switchers to generate the pipeline
    """

    data_name: str
    params: Dict[str, str] = field(default_factory=dict)
    switchers: Dict[str, str] = field(default_factory=dict)
    __pipeline: List[str] = field(default_factory=list)
    __raw: Data = None
    __processed: Data = None

    @staticmethod
    def _get_class_attributes(class_process):
        """Returns the attributes required to instanciate a class"""
        attributes = inspect.getmembers(
            class_process, lambda a: not inspect.isroutine(a)
        )
        return [
            a[0]
            for a in attributes
            if not (a[0].startswith("__") and a[0].endswith("__"))
        ]

    def _get_parameter_value(self, type_data, attributes):
        """Get parameter values"""
        parameters = {attr: self.params[type_data].get(attr) for attr in attributes}
        return {key: value for key, value in parameters.items() if value}

    def _generate_parameters(self, process):
        """Generate parameters dict"""
        attributes = self._get_class_attributes(process)
        global_parameters = self._get_parameter_value("global", attributes)
        process_parameters = self._get_parameter_value(self.data_name, attributes)
        return dict(global_parameters, **process_parameters)

    def _get_init_function(self, process):
        """Return the initialization fucntion"""
        return DATA_PROCESS_MAP[self.data_name][process]

    def init_raw(self):  # sourcery skip: class-extract-method
        """Initialize raw class"""
        data_class = self._get_init_function("raw")
        parameters = self._generate_parameters(data_class())
        self.__raw = data_class(**parameters)
        return self.__raw

    def init_processed(self):
        """Initialize processed class"""
        data_class = self._get_init_function("processed")
        parameters = self._generate_parameters(data_class())
        self.__processed = data_class(**parameters)
        return self.__processed

    def get_pipeline_order(self):
        """Return pipeline order"""
        return [process for process in self.switchers if self.switchers[process]]

    def map_data_process(self, process):
        """Map the process initialization functions"""
        processes = {
            "raw": self.init_raw,
            "processed": self.init_processed,
        }
        return processes[process]()

    def generate_pipeline(self):
        """Generate pipeline to process data"""
        pipeline_order = self.get_pipeline_order()
        for process in pipeline_order:
            self.__pipeline.append(self.map_data_process(process))

    def run(self):
        """Run pipeline"""
        self.generate_pipeline()
        for process in self.__pipeline:
            process.run()
