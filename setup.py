from setuptools import setup, find_packages

VERSION = '0.0.4'

with open('README.md', 'r') as f:
    long_description = f.read()

dependencies = [
    'google-cloud-bigquery >= 3.0.0, < 4.0.0',
    'google-cloud-storage >= 3.0.0, < 4.0.0'
]

setup(
    name='bqtoolkit',
    version=VERSION,
    description='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Federico Schmidt',
    author_email='schmidt.fdr@gmail.com',
    url='https://github.com/schmidtfederico/bqtoolkit',
    packages=find_packages(exclude=['tests']),
    install_requires=dependencies,
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
        'Operating System :: OS Independent',
        'Topic :: Internet'
    ],
    python_requires='>=3.8, <4'
)
