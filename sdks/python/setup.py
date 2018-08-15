import setuptools

with open("../../README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="beam-extended",
    version="0.0.3",
    author="Pascal Gula",
    author_email="pascal@plantix.net",
    description="Extend Apache Beam python API with new modules",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/PEAT-AI/beam-extended",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 2",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ),
)