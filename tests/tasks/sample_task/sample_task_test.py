import unittest
from sample.tasks.sample_task import SampleTask


class SampleTaskTest(unittest.TestCase):
    def test_sample_task(self):
        st = SampleTask()
        st.run()
