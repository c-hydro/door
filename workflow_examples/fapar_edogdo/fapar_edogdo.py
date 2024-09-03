from door.tools.config import Options
from door.tools.timestepping import TimeRange
from door import Downloader

json_file = '/home/luca/Documents/CIMA_code/door/workflow_examples/fapar_edogdo/fapar_edogdo.json'
options = Options.load(json_file)
dwl_options = options.get('downloader', ignore_case=True)

downloader = Downloader.from_options(dwl_options)
breakpoint()
downloader.get_data(TimeRange('2024-08-04', '2024-08-10'))
