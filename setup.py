import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="simple_pipes",
    version="0.0.1",
    author="Kevin Hill",
    author_email="kevin@humanpowered.tech",
    description="A simple pipeline system built around python iterators.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kevhill/simple-pipes",
    packages=setuptools.find_packages(),
    classifiers=(
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ),
)
