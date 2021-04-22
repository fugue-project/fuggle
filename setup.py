from setuptools import setup, find_packages
from fuggle_version import __version__


with open("README.md") as f:
    LONG_DESCRIPTION = f.read()

setup(
    name="fuggle",
    version=__version__,
    packages=find_packages(),
    description="Fugue for Kaggle users",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    author="Han Wang",
    author_email="goodwanghan@gmail.com",
    keywords="fugue kaggle sql spark dask pandas",
    url="http://github.com/fugue-project/fuggle",
    install_requires=[
        "fugue[spark,dask,sql]==0.5.3",
        "tune[all]==0.0.3",
        "notebook",
        "kaggle",
        "seaborn",
        "qpd",
        "dask[dataframe]",
    ],
    extras_require={},
    classifiers=[
        # "3 - Alpha", "4 - Beta" or "5 - Production/Stable"
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
    ],
    python_requires=">=3.6",
)
