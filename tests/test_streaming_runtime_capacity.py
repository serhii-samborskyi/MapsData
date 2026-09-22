import sys
import types
import unittest


try:
    import psycopg2.extras  # noqa: F401
except ModuleNotFoundError:
    psycopg2 = types.ModuleType("psycopg2")
    psycopg2_extras = types.ModuleType("psycopg2.extras")
    psycopg2_extras.Json = lambda value: value
    sys.modules["psycopg2"] = psycopg2
    sys.modules["psycopg2.extras"] = psycopg2_extras
    for module_name in ("streaming", "streaming_export", "streaming_services"):
        sys.modules[module_name] = types.ModuleType(module_name)

import streaming_runtime


class SharedWorkCapacityTests(unittest.TestCase):
    def setUp(self):
        self.capacity = streaming_runtime._SharedWorkCapacity()

    def test_limit_is_shared_and_releases_completed_work(self):
        self.capacity.configure(2)

        self.assertTrue(self.capacity.try_acquire())
        self.assertTrue(self.capacity.try_acquire())
        self.assertFalse(self.capacity.try_acquire())

        self.capacity.release()
        self.assertTrue(self.capacity.try_acquire())

    def test_limit_is_clamped_to_supported_range(self):
        self.capacity.configure(-1)
        self.assertTrue(self.capacity.try_acquire())
        self.assertFalse(self.capacity.try_acquire())


if __name__ == "__main__":
    unittest.main()
