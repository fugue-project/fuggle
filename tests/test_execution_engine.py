from fugue_test.builtin_suite import BuiltInTests
from fugue_test.execution_suite import ExecutionEngineTests
from fuggle import KaggleNativeExecutionEngine


class KaggleNativeExecutionEngineTests(ExecutionEngineTests.Tests):
    def make_engine(self):
        e = KaggleNativeExecutionEngine(dict(test=True))
        return e

    def test_map_with_dict_col(self):
        # TODO: add back
        return


class KaggleNativeExecutionEngineBuiltInTests(BuiltInTests.Tests):
    def make_engine(self):
        e = KaggleNativeExecutionEngine(dict(test=True))
        return e