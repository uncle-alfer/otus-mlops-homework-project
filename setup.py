from setuptools import find_packages, setup

setup(
   name='prediction',
   version='0.1.0',
   package_dir={"": "src"},
   packages=find_packages(where="src"),
#    install_requires=['wheel', 'bar', 'greek'], #external packages as dependencies
)