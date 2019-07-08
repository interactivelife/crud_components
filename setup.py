import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="crud_components",
    version="0.0.1",
    author="Mouhammed El Zaart, Jad Kik, Ahmad Sibai",
    author_email="mzaartdev@gmail.com",
    description="A package containing utilities that work on top of SQLAlchemy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
