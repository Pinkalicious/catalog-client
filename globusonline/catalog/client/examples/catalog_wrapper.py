#!/usr/bin/env python

from globusonline.catalog.client.goauth import get_access_token
from globusonline.catalog.client.dataset_client import DatasetClient

from globusonline.transfer.api_client import TransferAPIClient, Transfer

import re
import collections

class CatalogWrapper:
## Catalog wrapper wraps the features of the Globus Catalog API, and includes an interface to interact with Globus Transfer 

    def __init__(self, username=None, password=None,token=None, token_file=None):
        self.username        = username
        self.password        = password
        self.token           = token
        self.token_file      = token_file  

        self.catalogs        = ''
        self.datasets        = ''
        self.members         = ''
        self.transfer_details = []
       
        self.catalogBaseURL  = "https://catalog-alpha.globuscs.info/service/dataset"

        #Transfer Related Variables
        self.transferBaseURL = "https://transfer.test.api.globusonline.org/v0.10"
        self.destination_endpoint = ''
        self.destination_path   ='~'
        self.submission_id = ''
        self.transfer = ''
        self.active_endpoints = ''
        self.transfer_queue = ''
        
        #Client Variables
        self.catalogClient  = ''     #client for interfacing with Globus Catalog

        self.GO_authenticate()
        if(self.token):
            self.catalogClient = DatasetClient(self.token, self.catalogBaseURL)
            self.transferClient = TransferAPIClient(self.username, goauth=self.token, base_url=self.transferBaseURL)

    #Member Functions      
    def GO_authenticate(self):
        # Grab the access token, and store the token itself in GO_ACCESS_TOKEN
        tmpToken = ''

        if self.username is not None and self.password is not None:
            tmpToken = get_access_token(self.username, self.password)
            self.token = tmpToken.token
        elif self.token == 'file':
            try:
                file = open(self.token_file,'r')
                self.token = file.read()
                self.username = self.token.split('|')[0][3:] #read the username from the token file which is split by | and listed after un= in the first string
            except Exception, e:
                self.create_token_file()
                return True
            else:
                pass            
        else:
            tmpToken = get_access_token()
            self.token = tmpToken.token  
            self.username = self.token.split('|')[0][3:] #read the username from the token file which is split by | and listed after un= in the first string          

    def check_authenticate(self):
        #This could be a more robust check if necessary
        if self.token != '':
            return True
        else:
            return False

    def create_token_file(self):
        tmpToken = get_access_token()
        file = open(self.token_file,'w')
        file.write(tmpToken.token)
        self.token = tmpToken.token
        return True

    ##Transfer/Catalog Interfacing    
    def set_destination_endpoint(self, destination_endpoint=None):
        self.destination_endpoint = destination_endpoint
        return True

    def load_catalogs(self):
        _,self.catalogs = self.catalogClient.get_catalogs()
        return True

    def load_datasets(self, catalog_id):
        _,self.datasets = self.catalogClient.get_datasets(catalog_id)
        return True

    def load_members(self, catalog_id, dataset_id):
        _,self.members = self.catalogClient.get_members(catalog_id,dataset_id)
        return True
    
    def transfer_members(self, catalog_id, dataset_id, local_path=None):
        _,self.members = self.catalogClient.get_members(catalog_id,dataset_id)
        self.extract_transfer_details()
        self.activate_endpoints()

        for bundle in self.transfer_queue:
            print '==Unwrapping transfer bundle and creating transfer instance/ID %s => %s=='%(bundle[0]['endpoint'],self.destination_endpoint)
            #code, reason, result = self.transferClient.transfer_submission_id()
            #submission_id = result["value"]
            #transfer_object = Transfer(submission_id, bundle[0]['endpoint'], self.destination_endpoint)

            for transfer in bundle:
                print '==Starting transfer of %s @%s from %s to %s %s=='%(transfer['type'],transfer['location'],
                                                                          transfer['endpoint'],self.destination_endpoint, 
                                                                          self.destination_path)
                if transfer['type'] == 'file':
                    print '==Transferring File=='
                    #transfer_object.add_item(trans['location'], self.destination_path)
                elif transfer['type'] == 'directory':
                    print '==Transferring Directory -- Adding Recursion=='
                    #transfer_object.add_item(trans['location'], self.destination_path, recursive=True)
                else:
                    print '==Uncaught Type=='

            # status, reason, result = self.transferClient.transfer(transfer_object)
            # task_id = result["task_id"]
            # print '==TRANSFER TASK ID: %s=='%(result["task_id"])


            # status, reason, result = self.transferClient.task(task_id)
            # print '==TRANSFER RESULT: %s=='%(result["status"])

            #transfer the bundle    

        # for member in self.members:
        #     print '==START TRANSFER of %s from %s to %s=='%(member['data_type'],member['data_uri'],self.destination_endpoint)
        return True

    def extract_transfer_details(self):
        print '==Extracting Transfer Details=='
        #get the endpoints and data locations from the data_uri and store them in transfer_details    
        for member in self.members:
            tmpDict = {}
            match = re.findall('globus://([\w#]+)/([\w/~._-]+)',member['data_uri'])
            if match != []:
                tmpDict['endpoint'] = match[0][0]
                tmpDict['location'] = match[0][1]
                tmpDict['type']     = member['data_type']
                self.transfer_details.append(tmpDict)
        self.sort_transfers()
        return True

    def sort_transfers(self):
        sorted_transfers = collections.defaultdict(list)
        print '==Sorting Transfers=='
        for detail in self.transfer_details:
            sorted_transfers[detail['endpoint']].append(detail)
        self.transfer_queue = sorted_transfers.values()

    def activate_endpoints(self):
        endpoints = []
        unique_endpoints = []

        endpoints.append(self.destination_endpoint)
        for detail in self.transfer_details:
            endpoints.append(detail['endpoint'])

        unique_endpoints = list(set(endpoints))
        for endpoint in unique_endpoints:
            print "==Activating Endpoint",endpoint,"=="
            status, message, data = self.transferClient.endpoint_autoactivate(endpoint)
            #print data

###

