import sys
from setuptools import setup


DESCRIPTION = "seaborn: statistical data visualization"
LONG_DESCRIPTION = """
Seaborn is a library for making statistical graphics in Python. It is built on top of `matplotlib <https://matplotlib.org/>`_ and closely integrated with `pandas <https://pandas.pydata.org/>`_ data structures.

Here is some of the functionality that seaborn offers:

- A dataset-oriented API for examining relationships between multiple variables
- Specialized support for using categorical variables to show observations or aggregate statistics
- Options for visualizing univariate or bivariate distributions and for comparing them between subsets of data
- Automatic estimation and plotting of linear regression models for different kinds dependent variables
- Convenient views onto the overall structure of complex datasets
- High-level abstractions for structuring multi-plot grids that let you easily build complex visualizations
- Concise control over matplotlib figure styling with several built-in themes
- Tools for choosing color palettes that faithfully reveal patterns in your data

Seaborn aims to make visualization a central part of exploring and understanding data. Its dataset-oriented plotting functions operate on dataframes and arrays containing whole datasets and internally perform the necessary semantic mapping and statistical aggregation to produce informative plots.
"""

DIST_NAME = 'stockprophet'
MAINTAINER = 'ChiHyi Liao'
MAINTAINER_EMAIL = 'chihyi.liao@gmail.com'
LICENSE = 'BSD (3-clause)'
DOWNLOAD_URL = 'https://github.com/mwaskom/seaborn/'
VERSION = '0.1.0.dev0'
PYTHON_REQUIRES = ">=3.5"

INSTALL_REQUIRES = [
    'click>=7.1.2',
    'requests>=2.23.0',
    'lxml>=4.5.2',
    'SQLAlchemy>=1.13.17',
    'psycopg2-binary>= 2.8.5',
    'PyMySQL>==0.10.1',
    'configobj>=5.0.6',
    'numpy>=1.18.3'
]


PACKAGES = [
    'stockprophet',
    'stockprophet.db',
    'stockprophet.db.model',
    'stockprophet.db.manager',
    'stockprophet.crawler',
    'stockprophet.crawler.utils',
    'stockprophet.cli',
    'stockprophet.cli.ba',
    'stockprophet.cli.ta',
    'stockprophet.cli.stock'
]

CLASSIFIERS = [
    'Intended Audience :: Science/Research',
    'Programming Language :: Python :: 3.5'
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'License :: OSI Approved :: MIT License',
    'Topic :: Scientific/Engineering :: Visualization',
    'Topic :: Multimedia :: Graphics',
    'Operating System :: OS Independent',
    'Framework :: SQLAlchemy',
]

ENTRY_POINTS = {
    'console_scripts': ['stockprophet=stockprophet.__main__:main']
}


if sys.version_info[:2] < (3, 5):
    raise RuntimeError("stockprophet requires python >= 3.5.")


setup(
    name=DIST_NAME,
    author=MAINTAINER,
    author_email=MAINTAINER_EMAIL,
    maintainer=MAINTAINER,
    maintainer_email=MAINTAINER_EMAIL,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    license=LICENSE,
    version=VERSION,
    download_url=DOWNLOAD_URL,
    python_requires=PYTHON_REQUIRES,
    install_requires=INSTALL_REQUIRES,
    entry_points=ENTRY_POINTS,
    packages=PACKAGES,
    classifiers=CLASSIFIERS
)
