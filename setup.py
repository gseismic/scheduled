from setuptools import setup, find_packages

import os

def package_files(directory):
    paths = []
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))
    return paths

extra_files = package_files('scheduled/server')


setup(
    name='scheduled', 
    version='0.2.1', 
    packages=find_packages(),
    description='demo redis-based task schedule lib',
    install_requires = ['redis', 'logbook'],
    scripts=['scripts/scheduled_run.py'],
    python_requires = '>=3',
    include_package_data=True,
    package_data={
        '': extra_files
        # 'server': ['templates/*.html', 'static/*.*']  need templates/__init__.py
    },
    author='Liu Shengli',
    url='https://github.com/gseismic/scheduled',
    zip_safe=False,
    author_email='liushengli203@163.com'
)
