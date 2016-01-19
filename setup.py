from setuptools import setup, find_packages

requires = [
    'futures>=3.0.3',
    'requests'
]


setup(
    name='grabba_grabba_hey',
    version='1.0.0',
    author="J Gomez-Dans",
    author_email="j.gomez-dans@ucl.ac.uk",
    package_dir={'': 'grabba_grabba_hey'},
    packages=find_packages("grabba_grabba_hey"),
    install_requires=requires,
    zip_safe=False,
)