#!/usr/bin/env python

from globusonline.catalog.client.goauth import get_access_token
from globusonline.catalog.client.dataset_client import DatasetClient

class CatalogWrapper:
## Catalog wrapper wraps the features of the Globus Catalog API, and includes an interface to interact with Globus Transfer 

    #Init Function
    def __init__(self, username='', password='',token='', token_file=''):
        self.username        = username
        self.password        = password
        self.token           = token
        self.token_file      = token_file  
       
        self.catalogBaseURL  = "https://catalog-alpha.globuscs.info/service/dataset"

        #Transfer Related Variables
        self.transferBaseURL = "https://transfer.test.api.globusonline.org/v0.10"
        
        #Client Variables
        self.catalogClient  = ''     #client for interfacing with Globus Catalog

        self.GO_authenticate()
        if(self.token):
               self.catalogClient = DatasetClient(self.token, self.catalogBaseURL)
    ##END Init

    #Member Functions      
    def GO_authenticate(self):
        # Grab the access token, and store the token itself in GO_ACCESS_TOKEN
        tmpToken = ''

       

        if(self.username != '' and self.password != ''):
            tmpToken = get_access_token(self.username, self.password)
            self.token = tmpToken.token
        elif(self.token == 'file'):
            try:
                file = open(self.token_file,'r')    
                self.token = file.read()
            except Exception, e:
                self.create_token_file()
                return True
            else:
                pass            
        else:
            tmpToken = get_access_token()
            self.token = tmpToken.token           

    def check_authenticate(self):
        #This could be a more robust check if necessary
        if (self.token != ''):
            return True
        else:
            return False

    def create_token_file(self):
        tmpToken = get_access_token()

        #print 'Writing temporary token to file: %s'%('gotoken.txt')
        file = open(self.token_file,'w')
        file.write(tmpToken.token)
        self.token = tmpToken.token
        return True
###

