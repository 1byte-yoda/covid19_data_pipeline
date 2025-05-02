from setuptools import find_packages, setup

with open("README.md") as f:
    readme = f.read()


with open("requirements.txt") as f:
    requirements = f.readlines()


with open("requirements-dev.txt") as f:
    requirements_dev = f.readlines()


setup(
    name="covid_pipeline",
    long_description=readme,
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "covid_pipeline": [
            "dbt-project/**/*",
        ],
    },
    install_requires=requirements,
    extras_require={"dev": requirements_dev},
)
