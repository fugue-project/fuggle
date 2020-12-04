from typing import Any

from fugue_sql import FugueSQLWorkflow
from fuggle._engine_factory import ENGINE_FACTORY


class Dag(FugueSQLWorkflow):
    def __init__(self):
        super().__init__()

    def run(self, engine: Any = "native", conf: Any = None) -> None:
        super().run(ENGINE_FACTORY.make_engine(engine, conf))
