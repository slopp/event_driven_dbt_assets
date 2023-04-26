from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="event_driven_dbt_assets",
        packages=find_packages(),
        install_requires=[
            "dagster",
            "dagster-dbt",
            "pandas",
            "dbt-core",
            "dbt-duckdb",
            "dagster-duckdb",
        ],
        extras_require={"dev": ["dagit", "pytest"]},
    )