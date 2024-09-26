from setuptools import setup, find_packages

setup(
    name='SemT_py',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'numpy',
        'chardet',
        'PyJWT',
        'fake-useragent',
        'requests',  # Add other dependencies as needed
    ],
    author='Alidu Abubakari',
    author_email='a.abubakari@campus.unimib.it',
    description='A utility package for Semantic Enrichment of Tables',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/unimib-datAI/Semtui-python.git',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
    ],
)
