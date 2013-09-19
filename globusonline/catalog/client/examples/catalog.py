import sys
import json
import re
import os
from catalog_wrapper import *
from globusonline.catalog.client.operators import Op, build_selector

print_text = False  #Variable used to decide whether output should be in JSON (False) or limited plain text (True)

def split_args(the_command_list, the_args):
    global the_command

    for arg in the_args:
        if arg in the_command_list:
            the_command = arg
            split_index = the_args.index(arg) + 1
            return the_args[split_index:]

def check_flags(the_flag_list, the_args):
    global the_flags
    global print_text

    for arg in the_args:
        if arg in the_flag_list:
            the_flags.append(arg)
    for flag in the_flags:
        if flag == "-text":
            print_text = True

def format_catalog_text(the_catalog):
    #print catalog
    catalog_description = ''
    catalog_owners = ''
    catalog_name = ''
    try:
        catalog_description = the_catalog['config']['description']
    except KeyError, e:
        catalog_description = ''
    try:
        catalog_name = the_catalog['config']['name']
    except KeyError, e:
        catalog_name = 'no catalog name'
    return "%s)\t%s - [%s] - %s"%(the_catalog['id'], catalog_name, the_catalog['config']['owner'], catalog_description)

def format_dataset_text(the_dataset):
    dataset_labels = ''
    try:
        dataset_labels = ','.join(the_dataset['label'])
    except:
        dataset_labels = 'no labels'
    try:
        dataset_name = the_dataset['name']
    except KeyError, e:
        dataset_name = 'no dataset name'
    return "%s) %s - [%s] - <%s>"%(the_dataset['id'], dataset_name, the_dataset['owner'], dataset_labels)

def format_member_text(the_member):
    member_references = ''
    try:
        member_references = ','.join(the_member['dataset_reference'])
    except:
        member_labels = 'no references'
    return "%s) ref:%s - %s - %s"%(the_member['id'], member_references, the_member['data_type'], the_member['data_uri'])

def execute_commands(the_command, the_args):
    global wrap
    if(the_command == 'get_catalogs'):
        #takes no args, returns all visible catalogs
        try:
            _,catalog_list = wrap.catalogClient.get_catalogs()
            if print_text is True:
                print "============================================================"
                print "*More detailed catalog information available in JSON format*"
                print 'ID) Catalog Name - [Owner] - Catalog Description'
                print "============================================================"
                for catalog in catalog_list:
                   print format_catalog_text(catalog)
            else:
                print json.dumps(catalog_list)
            return True
        except KeyError, e:
            #print e
            return False
        else:
            return False
    
    elif(the_command == 'create_catalog'):
        try:
            arg_dict = json.loads(the_args[0])
            print "CREATE CATALOG - %s"%(arg_dict)
            catalog_list = wrap.catalogClient.create_catalog(arg_dict)
            return catalog_list
        except (IndexError, AttributeError):
            print "==================ERROR===================="
            print "Invalid Arguments passed for create_catalog"
            print "create_catalog accepts one argument 1) catalog property list"
            print "Example: python catalog.py create_catalog '{\"config\": {\"name\": \"JSON CAT NEW\"}}'"
            print "==========================================="
        except KeyError, e:
            print e
            return False
        else:
            return False

    elif(the_command == 'delete_catalog'):
        #@arg[0] = catalog ID -- INT
        #@arg[1] = verify -- True to verify deletion
        try:
            if(the_args[0] != '' and (the_args[1] == "True" or the_args[1]=="true" or the_args[1]==True)):
                print "DELETE CATALOG - Catalog ID:%s"%(the_args[0])
                wrap.catalogClient.delete_catalog(the_args[0])
                return True
        except IndexError:
                print "==================ERROR===================="
                print "Invalid Arguments passed for delete_catalog"
                print "delete_catalog accepts two arguments 1) the catalog ID and 2) a verification to delete (i.e. true)"
                print "Example: python catalog.py delete_catalog 1234 true"
                print "==========================================="
        except KeyError, e:
            print e
            return False
        else:
            return False

    elif(the_command == 'create_dataset'):
        #@arg[0] = catalog_id --  INT
        #@arg[1] = annotation list -- text string '{"name":"New Dataset"}'
        try:
            if(the_args[0] != '' and the_args[1] != ''):
                print "CREATE DATASET - Catalog ID:%s Annotations:%s"%(the_args[0],the_args[1])
                wrap.catalogClient.create_dataset(the_args[0],json.loads(the_args[1]))
                return True
        except IndexError, e:
                print "==================ERROR===================="
                print "Invalid Arguments passed for create_dataset"
                print "create_dataset accepts two arguments 1) the catalog ID and 2) a list of annotations"
                print "Example: python catalog.py create_dataset '{\"name\":\"New Dataset\"}'"
                print "==========================================="

        except KeyError, e:
            print 'Exception:',e
            return False
        else:
            return False

    elif(the_command == 'get_datasets'):
        #@arg[0] = catalog_id --  INT
        try:
            if(the_args[0] != ''):
                _,cur_datasets= wrap.catalogClient.get_datasets(the_args[0])
                
                if print_text is True:
                    print "============================================================"
                    print "*More detailed dataset information available in JSON format*"
                    print "ID) Dataset Name  - [Owner] - <Datset Labels>"
                    print "============================================================"
                    for dataset in cur_datasets:
                        print format_dataset_text(dataset)
            
                else:
                    print json.dumps(cur_datasets)
                return True
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for get_datasets"
            print "create_dataset accepts one arguments 1) the catalog ID"
            print "Example: python catalog.py get_datasets 17"
            print "==========================================="
        except KeyError, e:
            print e
            return False
        else:
            return False

    elif(the_command == 'create_annotation_def'):
        #@arg[0] = catalog_id -- INT
        #@arg[1] = annotation name - string
        #@arg[2] = value_type 
        try:
            print "CREATE ANNOTATION DEF - Catalog ID:%s  Name:%s  Type:%s"%(the_args[0],the_args[1],the_args[2])
            response = wrap.catalogClient.create_annotation_def(catalog_id=the_args[0], annotation_name=the_args[1], 
                                                                value_type=the_args[2])
            print response
            return response
        except KeyError, e:
            print e
            return False
        else:
            return False

    elif(the_command == 'get_annotation_defs'):
        #@arg[0] = catalog_id -- INT
        annotation_defs = ''
        arg0 = ''
        arg1 = ''

        try:
            arg0 = the_args[0]
        except:
            arg0 = ''
        try:
            arg1 = the_args[1]
        except:
            arg1 = ''
        try:
            _,annotation_defs = wrap.catalogClient.get_annotation_defs(arg0)
        except KeyError, e:
            print e

        if print_text is True:
            print "============================================================"
            print "Current catalog Annotations "
            print "============================================================"
            for annotation in annotation_defs:
                print annotation
        else:
            print json.dumps(annotation_defs) 

    elif(the_command == 'get_dataset_annotations'):
        #@arg[0] = catalog_id -- INT
        #@arg[1] = dataset id -- INT
        dataset_annotations = ''
        arg0 = ''
        arg1 = ''

        try: 
            arg0 = the_args[0]
        except:
            arg0 = ''
        try:
            arg1 = the_args[1]
        except: 
            arg1 = ''

        if(arg0 != '' and arg1 != ''):
            _,tmp_result = wrap.catalogClient.get_dataset_annotations(arg0,arg1,annotation_list=['annotations_present'])
            _,dataset_annotations = wrap.catalogClient.get_dataset_annotations(arg0,arg1,annotation_list=tmp_result[0]['annotations_present'])
        #elif(arg0 != '' and arg1 == ''):
         #   _,tmp_result = wrap.catalogClient.get_dataset_annotations(arg0,annotation_list=['annotations_present'])
            #_,dataset_annotations = wrap.catalogClient.get_dataset_annotations(arg0,tmp_result[0]['annotations_present'])
        else:
            print 'Invalid arguments passed'
            return False

        if print_text is True:
            print "Tag:Value"
            for record in dataset_annotations:
                for tag_key in record:
                    print "%s:%s"%(tag_key, record[tag_key])
        else:   
            print json.dumps(dataset_annotations)
        return True

    elif(the_command == 'add_dataset_tag'):
        #@arg[0] = catalog_id -- INT
        #@arg[1] = dataset id -- INT
        #@arg[2] = annotation list -- text string '{new-attribute:value}'
        try:
            if(the_args[0] != '' and the_args[1] != '' and the_args[2] != ''):
                #print "ADD DATASET TAG - Catalog ID:%s Dataset ID:%s Annotations:%s",(the_args[0],the_args[1],the_args[2])
                _,response = wrap.catalogClient.add_dataset_annotations(the_args[0],the_args[1],json.loads(the_args[2]))
                print response
                return True
        except KeyError, e:
            print e
            return False
    
    elif(the_command == 'delete_dataset'):
        #@arg[0] = catalog ID -- INT
        #@arg[1] = dataset ID -- INT
        #@arg[2] = verify -- True to verify deletion
        try:
            if(the_args[0] != '' and the_args[1] != '' and (the_args[2] == "True" or the_args[2]=="true" or the_args[2]==True)):
                print "DELETE DATASET - Catalog ID:%s Dataset ID: %s"%(the_args[0],the_args[1])
                wrap.catalogClient.delete_dataset(the_args[0],the_args[1])
                return True
        except KeyError, e:
            print e
            return False
        else:
            return False

    elif(the_command == 'get_dataset_members'):
        #@arg[0] = catalog ID -- INT
        #@arg[1] = dataset ID -- INT
        try:
            if the_args[0] != '' and the_args[1] != '':
                _,cur_members = wrap.catalogClient.get_members(the_args[0],the_args[1])
                if print_text is True:
                    print "============================================================"
                    print "*More detailed member information available in JSON format*"
                    print 'ID) Reference Dataset - Member Type - URI'
                    print "============================================================"
                    for member in cur_members:
                       print format_member_text(member)
                else:
                    print json.dumps(cur_members)
                return True
        except KeyError, e:
            print e
            return False
        else:
            return False

    elif(the_command == 'query_datasets'):
        selector_list = [(the_args[1],Op[the_args[2]],the_args[3])]
        _,result = wrap.catalogClient.get_datasets(the_args[0], selector_list=selector_list)
        if print_text is True:
            for dataset in result:
                print format_dataset_text(dataset)
        else:
            print json.dumps(result)

    elif(the_command == 'create_token_file'):
        wrap.create_token_file()
        return True

    else:
        print 'Invalid Command'
        return False

if __name__ == "__main__":

    command_list = ("get_catalogs","get_dataset_members","write_token",
                    "create_dataset","create_catalog","get_datasets",
                    "add_dataset_tag","create_annotation_def",
                    "delete_catalog","delete_dataset","get_dataset_annotations","get_member_annotations",
                    "get_annotation_defs","test_command","get_datasets_by_name","query_datasets")
    flag_list = ("-text")
    the_command = ''    #Stores the command to be executed via the catalogClient API
    selector_list = []
    the_flags = []      #Stores any flags detected in the arguments
    the_args = ''

    #Store authentication data in a local file
    token_file = os.getenv('HOME','')+"/.ssh/gotoken.txt"
    wrap = CatalogWrapper(token="file", token_file=token_file)
    
    #Input authentication data with every call
    #wrap = CatalogWrapper()

    #Check for any recognized flags from flag_list
    check_flags(flag_list,sys.argv)

    #Condition the input argument list. Slice at the first recognized command from command_list and set the_command
    the_args = split_args(command_list,sys.argv)

    #Execute the apprpriate client action and check for appropriate args
    execute_commands(the_command,the_args)