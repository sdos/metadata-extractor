# mcm-metadataExtractor
Metadata Extractor - part of the Micro Content Management system (MCM)


MCM consists of multiple components that form a small content management system.

### The other parts of the MCM project are
* [-- Deploy Environment (set up everything) --](https://github.com/timwaizenegger/mcm-deployEnvironment)
* [Bluebox web-UI](https://github.com/timwaizenegger/mcm-bluebox)
* [SDOS (Secure Delete Object Store) Cryptographic Deletion](https://github.com/timwaizenegger/mcm-sdos)
* [Metadata Extractor](https://github.com/timwaizenegger/mcm-metadataExtractor)
* [Retention Manager](https://github.com/timwaizenegger/mcm-retentionManager)


### This repo contains the metadata extractor.

extracts metadata from various file types. reads files from swift and writes structured metadata back


### configuration
is currently done by setting parameters in

     mcm/retentionManager/appConfig.py


## Dev setup
### first setup after new checkout
make sure to specify a python 3 or higher interpreter for your virtualenv (MCM doesn't support python 2)
in the main directory


    virtualenv venvMcmExtractor
    . setenv.sh
    (included in setenv) source venvMcmExtractor/bin/activate
    pip install -r requirements.txt
    

 
to leave venv

    deactivate
    
    
### running after first setup
in the main directory


    . setenv.sh
    python Tester.py
    (or any other class...)
    
    
### use pip to install requirements
just install the existing reqs

    pip install -r requirements.txt
    
install new packages

    pip install <package>


save new packages to requirements:

    pip freeze --local > requirements.txt