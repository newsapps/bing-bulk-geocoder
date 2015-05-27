from setuptools import find_packages, setup

# PyPI only supports nicely-formatted README files in reStructuredText.
# Newsapps seems to prefer Markdown.  Use a version of the pattern from
# https://coderwall.com/p/qawuyq/use-markdown-readme-s-in-python-modules
# to convert the Markdown README to rst if the pypandoc package is
# present.
try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst')
except (IOError, ImportError, OSError):
    long_description = open('README.md').read()

# Load the version from the version module
exec(open('bing_geocoder/version.py').read())

setup(
    name='bing_geocoder',
    version=__version__,
    author='Abraham Epton and Geoff Hing for the Chicago Tribune News Applications Team',
    author_email='newsapps@tribune.com',
    url='https://github.com/newsapps/bing-bulk-geocoder',
    description='Python library to make bing bulk geocoding a wee bit easier',
    packages=find_packages(exclude=["tests", "tests.*"]),
    include_package_data=True,
    install_requires=[
        'requests',
        'click',
        'pytz',
    ],
    tests_require=[
        'nose',
    ],
    entry_points='''
        [console_scripts]
        bing_geocoder=bing_geocoder.cli:cli
    ''',
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
)
