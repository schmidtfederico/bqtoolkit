from setuptools import setup, find_packages

VERSION = '0.0.3'

with open('README.md', 'r') as f:
    long_description = f.read()

dependencies = [
    'google-cloud-bigquery >= 1.24.0, < 3.0.0',
    'google-cloud-storage >= 1.26.0, < 3.0.0',
    'importlib-metadata<5.0'
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
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Operating System :: OS Independent',
        'Topic :: Internet'
    ],
    python_requires='>=3.6, <4'
)
