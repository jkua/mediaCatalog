from setuptools import setup, find_packages

setup(
    name='mediaCatalog',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pyyaml',
        'jsonlines',
        'packaging',
        'pyexiftool',
        'pyacoustid',
        'google-cloud-storage',
        'google-crc32c'
    ],
    scripts=[
        'scripts/catalog.py', 
        'scripts/queryCatalog.py', 
        'scripts/cloudUpload.py', 
        'scripts/cloudDownload.py'
    ],
    author='John Kua',
    author_email='john@kua.fm',
    description='Media file cataloger with cloud storage management',
    license='MIT',
    keywords='media file catalog cloud storage',
    url='https://github.com/jkua/mediaCatalog',
)
