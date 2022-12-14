# import multiprocessing to avoid this bug (http://bugs.python.org/issue15881#msg170215)
import multiprocessing
assert multiprocessing
import re
from setuptools import setup, find_packages


def get_version():
    """
    Extracts the version number from the version.py file.
    """
    VERSION_FILE = 'solarprod/version.py'
    mo = re.search(r'^__version__ = [\'"]([^\'"]*)[\'"]', open(VERSION_FILE, 'rt').read(), re.M)
    if mo:
        return mo.group(1)
    else:
        raise RuntimeError('Unable to find version string in {0}.'.format(VERSION_FILE))


install_requires = [
]

tests_require = [
]

docs_require = [
]

extras_require = {
    'dev': tests_require + docs_require,
}

setup(
    name='solarprod',
    version=get_version(),
    description='Solar production tools',
    long_description='Solar production tools',
    url='https://github.com/robdmc/solarprod',
    author='Rob deCarvalho',
    author_email='unlisted@unlisted.net',
    keywords='',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    license='MIT',
    include_package_data=True,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require=extras_require,
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'bodhi.find_detections = solarprod.scripts:find_detections',
            'bodhi.ibis = solarprod.scripts:ibis_connection',
            'bodhi.streamlit = solarprod.scripts:streamlit',
        ]
    }
)
