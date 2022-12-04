import click
from .pipelines import run_detector_pipeline
from .constants import VALID_CONNECTION_NAMES

@click.command()
@click.option('--ram-friendly/--ram-hostile', default=True, help='ram-hostile will load entire history table into ram (default friendly')
@click.option('--progress-bar/--no-progress-bar', default=False, help='Show progress bar (default no bar)')
def find_detections(ram_friendly, progress_bar):
    run_detector_pipeline(ram_friendly, show_progress_bar=progress_bar)


# if __name__ == '__main__':
#     main()

@click.command()
@click.argument('name')
def ibis_connection(name):
    if name not in VALID_CONNECTION_NAMES:
        raise ValueError(f'name must be one of {VALID_CONNECTION_NAMES}')
    from IPython import embed
    from traitlets.config import get_config
    from .ibis_tools import get_connections

    print(name)
    with get_connections(name) as conn:
        c = get_config()
        c.InteractiveShellEmbed.colors = "Linux"
        # embed(config=c)
        embed(colors="neutral")
