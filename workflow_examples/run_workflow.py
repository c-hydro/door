import argparse

from d3tools import Options
from d3tools.timestepping import get_date_from_str

import door

def parse_arguments():
    """
    Parse command line arguments for the workflow.

    Returns:
        argparse.Namespace: Parsed command line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Run workflow with specified parameters",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('workflow_json', type=str, help='JSON file of the workflow')
    parser.add_argument('-s', '--start', type=str, help='Start date to download the data [YYYY-MM-DD]')
    parser.add_argument('-e', '--end',   type=str, help='End date to download the data [YYYY-MM-DD]')

    args = parser.parse_args()

    return args

def main():
    args = parse_arguments()

    # load the options from the json file
    options = Options.load(args.workflow_json)

    # set the start and end date
    start_date = get_date_from_str(args.start) if args.start else None
    end_date   = get_date_from_str(args.end)   if args.end   else None

    # create the downloader
    downloader:door.Downloader = door.Downloader.from_options(options.DOOR_DOWNLOADER)

    # run the computation
    downloader.get_data((start_date, end_date))
    
if __name__ == '__main__':
    main()