import os
import sys
from setuptools import setup


def read(relpath):
    filename = os.path.join(os.path.dirname(__file__), relpath)
    with open(filename) as f:
        return f.read()


setup(
    name='txpool',
    version='0.9',
    description='A persistent process pool for Twisted',
    long_description=read('README.rst'),
    license='MIT',
    author='Ryan Johnson',
    author_email='escattone@gmail.com',
    url='https://github.com/escattone/txpool',
    packages=['txpool'],
    install_requires=['twisted>=12'],
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 2 :: Only',
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
