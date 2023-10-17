# website:   https://www.brooklyn.health

import setuptools
import os
import sys
import logging

# Version details
current_dir = os.path.abspath(os.path.dirname(__file__))
log_file = os.path.join(current_dir, ".willisapilogs")
details = {}
with open(os.path.join(current_dir, "willisapi_client", "__version__.py"), "r") as fv:
    exec(fv.read(), details)

# Long description
with open("README.md", "r") as fh:
    long_description = fh.read()

# Dependencies
with open("requirements.txt", "r") as fp:
    install_requires = fp.read().split("\n")
    while "" in install_requires:
        install_requires.remove("")


def willisapi_package_log():
    logging.basicConfig(
        filename=log_file,
        level=logging.CRITICAL,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.critical("An error occurred: %s", e)


if __name__ == "__main__":
    try:
        setuptools.setup(
            name=details["__client__"],
            version=details["__latestVersion__"],
            description=details["__short_description__"],
            long_description=long_description,
            long_description_content_type=details["__content_type__"],
            url=details["__url__"],
            author="bklynhlth",
            python_requires=">=3.9",
            install_requires=install_requires,
            author_email="admin@brooklyn.health",
            packages=setuptools.find_packages(exclude=["tests*"]),
            include_package_data=True,
            zip_safe=False,
            license="Apache",
        )

    except Exception as e:
        willisapi_package_log("An error occurred: {}".format(e))
        sys.exit(1)
