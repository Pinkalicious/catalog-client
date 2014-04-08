from distutils.core import setup

setup(name="globusonline-catalog-api-client",
      version="0.1a1",
      description="Globus Online Catalog API client library",
      long_description=open("README.rst").read(),
      author="Bryce Allen",
      author_email="ballen@ci.uchicago.edu",
      url="https://github.com/globusonline/catalog-client",
      packages=["globusonline",
                "globusonline.catalog",
                "globusonline.catalog.client",
                "globusonline.catalog.client.ca",
                "globusonline.catalog.client.examples"],
      package_data={ "globusonline.catalog.client": ["ca/*.pem"] },
      scripts = ["globusonline/catalog/client/examples/catalog.py"],
      keywords = ["globusonline"],
      classifiers=[
          "Development Status :: 3 - Alpha",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: Apache Software License",
          "Operating System :: MacOS :: MacOS X",
          "Operating System :: Microsoft :: Windows",
          "Operating System :: POSIX",
          "Programming Language :: Python",
          "Topic :: Communications :: File Sharing",
          "Topic :: Internet :: WWW/HTTP",
          "Topic :: Software Development :: Libraries :: Python Modules",
          ],
      )
