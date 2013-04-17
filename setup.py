from setuptools import setup
import sitecat_py


setup(name='sitecat-py',
      author='Lex Hider',
      description="Pandas support for Adobe's SiteCatalyst",
      long_description=open('README.rst').read(),
      url='https://github.com/lexual/sitecat-py',
      version=sitecat_py.__version__,
      packages=['sitecat_py'],
      license='BSD',
      install_requires=['requests'])
