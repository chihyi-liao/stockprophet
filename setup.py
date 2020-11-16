import sys
from setuptools import setup


DESCRIPTION = "stockprophet: stock data statistical and analysis"
LONG_DESCRIPTION = """
StockProphet 是台股網路爬蟲和分析的工具.

以下為此工具的一些特點:

- 使用 SQLAlchemy 將抓取的股價歷史資料存入資料庫, 可用資料庫為 SQLite 或 MySQL 或 PostgreSQL
- 抓取的資料分別為 台灣上市櫃網站與股市公開交易站, 主要為每日歷史資料表, 每月營收表, 資產負債表以及綜合損益表
- 會將抓取的 每日歷史資料表 經過計算後存成 每週歷史資料表 與 每月歷史資料表
- 提供 cli 命令分析 基本面 與 技術面 並提供多種參數可以調校

"""

DIST_NAME = 'stockprophet'
MAINTAINER = 'ChiHyi Liao'
MAINTAINER_EMAIL = 'chihyi.liao@gmail.com'
LICENSE = 'BSD (3-clause)'
DOWNLOAD_URL = 'https://github.com/chihyi-liao/stockprophet/'
VERSION = '0.1.0.dev0'
PYTHON_REQUIRES = ">=3.5"

INSTALL_REQUIRES = [
    'click>=7.1.2',
    'requests>=2.23.0',
    'lxml>=4.5.2',
    'SQLAlchemy>=1.3.17',
    'psycopg2-binary>= 2.8.5',
    'PyMySQL>==0.10.1',
    'configobj>=5.0.6',
    'numpy>=1.18.3',
    'databases>=0.3.2'
]


PACKAGES = [
    'stockprophet',
    'stockprophet.db',
    'stockprophet.db.model',
    'stockprophet.db.manager',
    'stockprophet.db.manager.sync_api',
    'stockprophet.db.manager.async_api',
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
    'console_scripts': ['stockprophet=stockprophet.__init__:main']
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
