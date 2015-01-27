import os
import sys
from setuptools import setup


setup(
    name='txpool',
    version='0.9',
    description='A persistent process pool for Twisted',
    long_description=("A persistent process pool for Twisted that provides "
                      "the ability to asynchronously run Python callables "
                      "as long as the callable, its arguments, and its "
                      "return value are all picklable."),
    license='MIT',
    author='Ryan Johnson',
    author_email='escattone@gmail.com',
    url='http://github.com/escattone/txpool/',
    packages=['txpool'],
    install_requires=['twisted>=12'],
    classifiers=[
        'Programming Language :: Python',
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ]
)
