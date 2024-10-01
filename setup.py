from setuptools import setup, Command

from distutils.command.build_py import build_py

with open('README.md') as infile:
    long_description = infile.read()

from az_lfs_hsm_release import __version__

setup(name='az_lfs_hsm_release',
      version=__version__,
      description='Python package to allow release from Lustre HSM',
      long_description=long_description,
      url='https://github.com/wolfgang-desalvador/az-lfs-hsm-release',
      license='MIT License',
      author='Wolfgang De Salvador',
      author_email='',
      packages=['az_lfs_hsm_release'],
      provides=['az_lfs_hsm_release'],
      install_requires=['azure-storage-blob', 'azure-identity'],
      cmdclass={'build_py': build_py},
      entry_points={'console_scripts': ['az_lfs_hsm_release = az_lfs_hsm_release.main:main']},
      classifiers=[
                   "Development Status :: 3 - Alpha",
                   "Programming Language :: Python",
                   "License :: OSI Approved :: MIT License",
                  ],
     )