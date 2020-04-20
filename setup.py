from setuptools import setup, find_packages

VERSION = '0.0.2'

with open('README.md', 'r') as f:
    long_description = f.read()

dependencies = [
    'google-cloud-bigquery >= 1.24.0, < 2.0.0',
    'google-cloud-storage >= 1.26.0, < 2.0.0',
    "backports.tempfile;python_version<'3.4'"
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
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        "Programming Language :: Python :: 3.5",
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Operating System :: OS Independent',
        'Topic :: Internet'
    ],
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <4',
    extras_require={
        'testing:python_version == "2.7"': ['mock'],
        'testing:python_version == "3.5"': ['mock']
    }
)
