# website:   http://www.brooklyn.health

import setuptools
import glob

#Long description
with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fp:
    install_requires = fp.read()

setuptools.setup(name='willisapi_client',
                 version='0.1',
                 description='A Python client for willisapi',
                 long_description=long_description,
                 long_description_content_type="text/markdown",
                 url='https://github.com/bklynhlth/willisapi_client',
                 author='bklynhlth',
                 python_requires='>=3.6',
                 install_requires=install_requires,
                 author_email='admin@brooklyn.health',
                 packages=setuptools.find_packages(),
                 include_package_data=True,
                 zip_safe=False,
                 license='Apache'
                )
