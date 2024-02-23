from setuptools import find_packages, setup

setup(
   name='prediction',
   version='0.1.0',
   packages=find_packages(include=['src', 'src.*']),
#    install_requires=['wheel', 'bar', 'greek'], #external packages as dependencies
)