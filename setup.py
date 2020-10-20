from setuptools import setup, find_packages


setup(
    name='scheduled', 
    version='0.1.0', 
    packages=find_packages(),
    description='redis-based task schedule lib',
    install_requires = ['redis'],
    scripts=[],
    python_requires = '>=3',
    include_package_data=True,
    author='Liu Shengli',
    url='github.com',
    zip_safe=False,
    author_email='liushengli203@163.com'
)
