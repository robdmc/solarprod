import click
from .pipelines import run_detector_pipeline

@click.command()
@click.option('--ram-friendly/--ram-hostile', default=True, help='ram-hostile will load entire history table into ram')
def main(ram_friendly):
    print(f'memory_f={ram_friendly}')
    run_detector_pipeline(ram_friendly)



if __name__ == '__main__':
    main()

