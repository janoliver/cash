from setuptools import setup

setup(
    name='cascading-shell',
    version='1.2',
    packages=['cash'],
    license='GPL',
    author="Jan Oliver Oelerich",
    author_email='janoliver@oelerich.org',
    url='https://github.com/janoliver/cash',
    keywords=['shell', 'hpc', 'computer cluster'],
    classifiers=[
        'Environment :: Console',
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Operating System :: Unix',
        'Programming Language :: Python :: 3.6',
        'Topic :: System :: Distributed Computing',
        'Topic :: System :: Shells',
        'Topic :: System :: Systems Administration',
        'Topic :: Utilities'
    ],
    entry_points={
        'console_scripts': [
            'cash=cash.main:main',
        ],
    },
    extras_require = {
        'Colored output':  ["termcolor"]
    }
)
