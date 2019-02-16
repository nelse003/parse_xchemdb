import logging
import luigi
import os
import time
from luigi.contrib.sge import SGEJobTask

logger = logging.getLogger('luigi-interface')

class TestJobTask(SGEJobTask):

    i = luigi.Parameter()

    def work(self):
        logger.info('Running test job...')
        with open(self.output().path, 'w') as f:
            f.write('this is a test')
    def output(self):
        return luigi.LocalTarget(os.path.join(
            '/dls/science/groups/i04-1/elliot-dev/Work/'
            'exhaustive_parse_xchem_db/luigi_test',
            'testfile_' + str(self.i)))


if __name__ == '__main__':

    tmp_dir = '/dls/science/groups/i04-1/elliot-dev/Work/' \
              'exhaustive_parse_xchem_db/luigi_tmp'

    tasks = [TestJobTask(i=str(i),
                         n_cpu=i+1,
                         shared_tmp_dir=tmp_dir,
                         parallel_env='make',
                         dont_remove_tmp_dir=True) for i in range(6)]
    luigi.build(tasks, local_scheduler=True, workers=3)