import argparse
import logging

from d3tools import WorkflowDefinition
from d3tools.timestepping import TimeRange, TimeWindow

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
    parser.add_argument('-t', '--time', type=str, help='Reference timestep [YYYY-MM-DD HH:MM]')

    args = parser.parse_args()
    if args.time is not None and (args.start is not None or args.end is not None):
        parser.error("--time cannot be combined with --start or --end")

    return args


def reference_timerange(options: WorkflowDefinition, reference_time: str) -> TimeRange:
    """Resolve ``-t`` as one run or as the end of a native repeat window."""
    time_range = TimeRange.from_any([reference_time, reference_time])
    exec_options = options.options.get("exec_options", {}) or {}
    repeat_window = exec_options.get("repeat_window")
    if repeat_window:
        time_range = TimeWindow.from_str(str(repeat_window)).apply(time_range.start)
    return time_range

def main():
    args = parse_arguments()

    # load and parse the options from the json file
    options:WorkflowDefinition = WorkflowDefinition.load(
        args.workflow_json,
        build_workflow_objects=True,
        strict_workflow_imports=True,
    )

    if args.time is not None:
        options.run(time_range=reference_timerange(options, args.time))
    else:
        for wf_section in options.workflow_sections:
            wf_section.value.get_last_ts()
        options.run(start=args.start, end=args.end)


if __name__ == '__main__':
    main()
