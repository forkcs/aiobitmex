from setuptools import setup

from aiobitmex import constants

install_requires = [req[:-1] for req in open('requirements.txt').readlines()]

setup(
    name='aiobitmex',
    description='Async connector to the BitMEX API.',
    version=constants.VERSION,
    packages=['aiobitmex', 'aiobitmex.http', 'aiobitmex.ws'],
    install_requires=install_requires,
    url='https://github.com/forkcs/aiobitmex'
)
