from setuptools import setup, find_packages

setup(
    name="pricehistory",
    version="0.1.0",
    author="sajjad_shokrgozar",
    author_email="shokrgozarsajjad@gmail.com",
    description="Tehran's stock market daily price history",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/sajjad-shokrgozar/pricehistory_api",
    packages=find_packages(),
    package_data={'pricehistory': ['firms_info.csv']},
    install_requires=[
        "requests",
        "jdatetime"
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    license="MIT",
    license_files=["LICENSE"],  # ✅ Correct field
)
