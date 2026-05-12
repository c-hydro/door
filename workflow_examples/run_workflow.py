import argparse
import logging

from d3tools import WorkflowDefinition

main_log = logging.getLogger("main")
main_log.setLevel(logging.INFO)
handler = logging.StreamHandler()
main_log.addHandler(handler)

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

    # load and parse the options from the json file
    options:WorkflowDefinition = WorkflowDefinition.load(
        args.workflow_json,
        build_workflow_objects=True,
        strict_workflow_imports=True,
    )

    for wf_section in options.workflow_sections:
        wf_section.value.get_last_ts()

    # set the start and end date
    start_date = args.start
    end_date   = args.end

    # run the workflow
    options.run(start=start_date, end=end_date)


if __name__ == '__main__':
    main()