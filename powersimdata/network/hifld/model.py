import os

from powersimdata.input.abstract_grid import AbstractGrid
from powersimdata.network.csv_reader import CSVReader
from powersimdata.network.usa_tamu.constants.storage import defaults


class HIFLD(AbstractGrid):
    def __init__(self, interconnect):
        """Constructor."""
        super().__init__()
        self._set_data_loc()

        self.interconnect = check_and_format_interconnect(interconnect)
        self._build_network()

    def _set_data_loc(self):
        """Sets data location.

        :raises IOError: if directory does not exist.
        """
        top_dirname = os.path.dirname(__file__)
        data_loc = os.path.join(top_dirname, "data")
        if os.path.isdir(data_loc) is False:
            raise IOError("%s directory not found" % data_loc)
        else:
            self.data_loc = data_loc

    def _build_network(self):
        """Build network."""
        reader = CSVReader(self.data_loc)
        self.bus = reader.bus
        self.plant = reader.plant
        self.branch = reader.branch
        self.dcline = reader.dcline
        self.gencost["after"] = self.gencost["before"] = reader.gencost

        self.storage.update(defaults)

        add_information_to_model(self)

        if "USA" not in self.interconnect:
            self._drop_interconnect()

    def _drop_interconnect(self):
        """Trim data frames to only keep information pertaining to the user
        defined interconnect(s).

        """
        for key, value in self.__dict__.items():
            if key in ["sub", "bus2sub", "bus", "plant", "branch"]:
                value.query("interconnect == @self.interconnect", inplace=True)
            elif key == "gencost":
                value["before"].query(
                    "interconnect == @self.interconnect", inplace=True
                )
            elif key == "dcline":
                value.query(
                    "from_interconnect == @self.interconnect &"
                    "to_interconnect == @self.interconnect",
                    inplace=True,
                )
        self.id2zone = {k: self.id2zone[k] for k in self.bus.zone_id.unique()}
        self.zone2id = {value: key for key, value in self.id2zone.items()}


def check_and_format_interconnect(interconnect):
    # Placeholder for now
    return interconnect


def interconnect_to_name(interconnect):
    # Placeholder for now
    return interconnect
