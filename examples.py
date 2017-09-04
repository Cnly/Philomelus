import sys

import loggingconfigurer
import logging
from dataservice import *
from querysupplier import *
from persistenceservice import *

l = logging.getLogger(__name__)
l.info('main starting')

# To obtain score data using the default simple brute force query supplier
# SID range is from 0000000000 to 1234567890(both ends included)
# Using 20 threads for data obtaining and 4 threads for captcha pool preparing
# Then save it in CSV format, to file `output/test.csv`
# sds = SouthDataService(20, DefaultBruteForceQuerySupplier(1, 1234567891), captcha_worker_count=4)
# sds.start()
# cps = CSVPersistenceService(sds, 'output/test.csv')
# cps.start()

# To obtain score data using a local query supplier which extract query sets from a local log or csv file previously exported
# by Philomelus
# Other settings are the same as the above
# sds = SouthDataService(20, LocalQuerySupplier('output/test.csv'), captcha_worker_count=4)
# sds.start()
# cps = CSVPersistenceService(sds, 'output/test.csv')
# cps.start()

# To obtain enrollment data using a local query supplier
# seds = SouthEnrollmentDataService(20, LocalQuerySupplier('output/test.csv'), captcha_worker_count=4)
# seds.start()
# cps = CSVEnrollmentPersistenceService(seds, 'output/e_test.csv')
# cps.start()

# To import all sets of score data from single/multiple local files, merge them and make a new file
# Useful when needing to combine multiple output files or
# wanting to re-perform the ranking, etc. process
# local_files = (
#     'output/this_school.csv',
#     'output/that_school.csv'
# )
# lds = LocalDataService(20, *local_files)
# lds.start()
# cps = CSVPersistenceService(lds, 'output/test.csv')
# cps.start()