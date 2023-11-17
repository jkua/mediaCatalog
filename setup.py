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
        'scripts/mcat',
        'scripts/mcat-catalog.py', 
        'scripts/mcat-query.py',
        'scripts/mcat-verify.py',
        'scripts/mcat-remove.py', 
        'scripts/mcat-stats.py',
        'scripts/mcat-cloudUpload.py', 
        'scripts/mcat-cloudDownload.py',
        'scripts/mcat-getMetadata.py'
    ],
    author='John Kua',
    author_email='john@kua.fm',
    description='Media file cataloger with cloud storage management',
    license='MIT',
    keywords='media file catalog cloud storage',
    url='https://github.com/jkua/mediaCatalog',
)
