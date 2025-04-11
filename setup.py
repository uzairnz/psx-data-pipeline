from setuptools import setup, find_packages

setup(
    name="psx_data_automation",
    version="0.1.0",
    description="PSX Historical Data Automation",
    author="Uzair",
    author_email="github.com/uzairnz",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.3.4",
        "requests>=2.25.1",
        "beautifulsoup4>=4.9.3",
        "schedule>=1.1.0",
        "argparse>=1.4.0",
        "python-dateutil>=2.8.2",
        "lxml>=4.8.0",
        "numpy>=1.20.3",
        "matplotlib>=3.4.3",
        "tqdm>=4.62.3",
        "python-dotenv>=0.19.2",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
) 