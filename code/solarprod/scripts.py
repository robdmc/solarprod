import click
from .pipelines import run_detector_pipeline


@click.command()
@click.option('--ram-friendly/--ram-hostile', default=True, help='ram-hostile will load entire history table into ram (default friendly')
@click.option('--progress-bar/--no-progress-bar', default=False, help='Show progress bar (default no bar)')
def main(ram_friendly, progress_bar):
    run_detector_pipeline(ram_friendly, show_progress_bar=progress_bar)


if __name__ == '__main__':
    main()
