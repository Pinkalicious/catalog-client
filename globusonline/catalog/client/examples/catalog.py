import sys
import json
import re
import os
import ConfigParser
import io

from catalog_wrapper import *
from globusonline.catalog.client.operators import Op, build_selector

print_text = False  #Variable used to decide whether output should be in JSON (False) or limited plain text (True)
default_catalog = None 

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
    global default_catalog

    for arg in the_args:
        if arg in the_flag_list:
            the_flags.append(arg)
    for flag in the_flags:
        if flag == "-text":
            print_text = True
        if flag == "-default_catalog":
            default_catalog = os.getenv('DEFAULT_CATALOG_ID')


def format_catalog_text(the_catalog):
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

def execute_command(the_command, the_args):
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
            #print "CREATE CATALOG - %s"%(arg_dict)
            try:
                _,catalog_list = wrap.catalogClient.create_catalog(arg_dict)
                print catalog_list['id']
            except Exception, e:
                print e
                return False
            return True
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
        #Arguments f(catalog_id, annotation_list) 
        #annotation list -- text string '{"name":"New Dataset"}'
        catalog_arg = None
        annotation_arg = None

        try:
            if default_catalog is not None:
                catalog_arg = default_catalog
                annotation_arg = the_args[0]
            else:
                catalog_arg = the_args[0]
                annotation_arg = the_args[1]
        except IndexError, e:
            print "==================ERROR===================="
            print "Invalid Arguments passed for create_dataset"
            print "create_dataset accepts two arguments 1) the catalog ID and 2) a list of annotations"
            print "Example: python catalog.py create_dataset '{\"name\":\"New Dataset\"}'"
            print "==========================================="
        except KeyError:
            print 'KeyError:',e

        if catalog_arg is not None and annotation_arg is not None:
                #print "CREATE DATASET - Catalog ID:%s Annotations:%s"%(catalog_arg,annotation_arg)
                    try:
                        _,result = wrap.catalogClient.create_dataset(catalog_arg,json.loads(annotation_arg))
                        print "%s,%s"%(catalog_arg,result['id'])
                    except Exception, e:
                        print e
                
       

    elif(the_command == 'get_datasets'):
        #Arguments f(catalog_id)
        catalog_arg = None

        try:
            if default_catalog is not None:
                catalog_arg = default_catalog
            else:
                catalog_arg = the_args[0]
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for get_datasets"
            print "get_datasets accepts one arguments 1) the catalog ID"
            print "Example: python catalog.py get_datasets 17"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False

        if catalog_arg is not None:
            _,cur_datasets= wrap.catalogClient.get_datasets(catalog_arg)
            
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


    elif(the_command == 'create_annotation_def'):
        #Arguments f(catalog_id, annotation_name, value_type)
        catalog_arg = None
        annotation_arg = None
        value_arg = None
        multivalue_arg = False

        try:
            if default_catalog is not None:
                catalog_arg = default_catalog
                annotation_arg = the_args[0]
                value_arg = the_args[1]
                try:
                    multivalue_arg = the_args[2]
                except IndexError:
                    pass
            else:
                catalog_arg = the_args[0]
                annotation_arg = the_args[1]
                value_arg = the_args[2]
                try:
                    multivalue_arg = the_args[3]
                except IndexError:
                    pass

        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for create_annotation_def"
            print "create_dataset accepts three arguments 1) the catalog ID"
            print "2) Annotation Name 3) Annotation Value"
            print "Example: python catalog.py create_annotation_def 48 'datalocation' 'text'"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False

        bool_multivalue_arg = multivalue_arg in ['true','True','t','T','1']    
        print "CREATE ANNOTATION DEF - Catalog ID:%s  Name:%s  Type:%s"%(catalog_arg,annotation_arg,value_arg)
        response = wrap.catalogClient.create_annotation_def(catalog_id=catalog_arg, annotation_name=annotation_arg, 
                                                            value_type=value_arg, multivalued=bool_multivalue_arg)
        print response
        return response
        

    elif(the_command == 'get_annotation_defs'):
        #Arguments f(catalog_id)
        catalog_arg = None

        try:
            if default_catalog is not None:
                catalog_arg = default_catalog
            else:
                catalog_arg = the_args[0]
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for get_annotation_defs"
            print "get_annotation_defs accepts one arguments 1) the catalog ID"
            print "Example: python catalog.py get_annotation_defs 17"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False

        if catalog_arg:
            _,annotation_defs = wrap.catalogClient.get_annotation_defs(catalog_arg)
       

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
        catalog_arg = None
        dataset_arg = None

        try:
            if default_catalog:
                catalog_arg = default_catalog
                dataset_arg = the_args[0]
            else:
                catalog_arg = the_args[0]
                dataset_arg = the_args[1]
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for get_dataset_annotations"
            print "get_dataset_annotations accepts two arguments"
            print "1) the catalog ID and 2) the dataset ID"
            print "Example: python catalog.py get_annotation_defs 17"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False

        if catalog_arg and dataset_arg:
            _,tmp_result = wrap.catalogClient.get_dataset_annotations(catalog_arg,dataset_arg,annotation_list=['annotations_present'])
            _,dataset_annotations = wrap.catalogClient.get_dataset_annotations(catalog_arg,dataset_arg,annotation_list=tmp_result[0]['annotations_present'])
        else:
            print 'Invalid arguments passed'
            return False

        if print_text is True:
            print "Tag:Value"
            for record in dataset_annotations:
                for annotation_key in record:
                    print "%s:%s"%(annotation_key, record[annotation_key])
        else:   
            print json.dumps(dataset_annotations)
        return True

    elif(the_command == 'add_dataset_annotation'):
        #@arg[0] = catalog_id -- INT
        #@arg[1] = dataset id -- INT
        #@arg[2] = annotation list -- text string '{new-attribute:value}'
        catalog_arg = None
        dataset_arg = None
        annotation_arg = None

        try:
            if default_catalog:
                catalog_arg = default_catalog
                dataset_arg = the_args[0]
                annotation_arg = the_args[1]
            else:
                catalog_arg = the_args[0]
                dataset_arg = the_args[1]
                annotation_arg = the_args[2]
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for add_dataset_annotation"
            print "add_dataset_annotation accepts three arguments"
            print "1) the catalog ID and 2) the dataset ID 3) Annotation list"
            print "Example: python catalog.py add_dataset_annotation 17 54 '{\"test-annotation\":\"true\",\"material\":\"copper\"}'"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False


        if catalog_arg and dataset_arg and annotation_arg:
            #print "ADD DATASET TAG - Catalog ID:%s Dataset ID:%s Annotations:%s",(the_args[0],the_args[1],the_args[2])
            _,response = wrap.catalogClient.add_dataset_annotations(catalog_arg,dataset_arg,json.loads(annotation_arg))
            print response
            return True

    
    elif(the_command == 'delete_dataset'):
        #@arg[0] = catalog ID -- INT
        #@arg[1] = dataset ID -- INT
        #@arg[2] = verify -- True to verify deletion
        catalog_arg = None
        dataset_arg = None
        verify_arg = None


        try:
            if default_catalog:
                catalog_arg = default_catalog
                dataset_arg = the_args[0]
                verify_arg = the_args[1]
            else:
                catalog_arg = the_args[0]
                dataset_arg = the_args[1]
                verify_arg = the_args[2]
        except IndexError,e:
            print 'Index Error:',e
            return False
        except KeyError, e:
            print e
            return False

        if catalog_arg and dataset_arg and (verify_arg == "True" or verify_arg=="true" or verify_arg==True):
            print "DELETE DATASET - Catalog ID:%s Dataset ID: %s"%(catalog_arg,dataset_arg)
            wrap.catalogClient.delete_dataset(catalog_arg,dataset_arg)
            return True
       

    elif(the_command == 'get_dataset_members'):
        #@arg[0] = catalog ID -- INT
        #@arg[1] = dataset ID -- INT
        catalog_arg = None
        dataset_arg = None

        try:
            if default_catalog:
                catalog_arg = default_catalog
                dataset_arg = the_args[0]
            else:
                catalog_arg = the_args[0]
                dataset_arg = the_args[1]
        except IndexError,e:
            print e

        if catalog_arg and dataset_arg:
            _,cur_members = wrap.catalogClient.get_members(catalog_arg,dataset_arg)
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

    elif(the_command == 'add_member_annotation'):
        #@arg[0] = catalog_id -- INT
        #@arg[1] = dataset id -- INT
        #@arg[2] = annotation list -- text string '{new-attribute:value}'
        catalog_arg = None
        dataset_arg = None
        member_arg = None
        annotation_arg = None

        try:
            if default_catalog:
                catalog_arg = default_catalog
                dataset_arg = the_args[0]
                member_arg = the_args[1]
                annotation_arg = the_args[2]
            else:
                catalog_arg = the_args[0]
                dataset_arg = the_args[1]
                member_arg = the_args[2]
                annotation_arg = the_args[3]
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for add_member_annotation"
            print "add_member_annotation accepts four arguments"
            print "1) the catalog ID and 2) the dataset ID 3) the member ID"
            print "4) the annotations (JSON)"
            print "Example: python catalog.py add_dataset_annotation 17 54'{\"test-annotation\":\"true\",\"material\":\"copper\"}'"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False

        if catalog_arg and dataset_arg and member_arg and annotation_arg:
            _,response = wrap.catalogClient.add_member_annotations(catalog_arg,dataset_arg,member_arg,json.loads(annotation_arg))
            print response
            return True


    elif(the_command == 'query_datasets'):
        catalog_arg = None
        field_arg = None
        operator_arg = None
        value_arg = None

        try:
            if default_catalog:
                catalog_arg = default_catalog
                field_arg = the_args[0]
                operator_arg = the_args[1]
                value_arg = the_args[2]
            else:
                catalog_arg = the_args[0]
                field_arg = the_args[1]
                operator_arg = the_args[2]
                value_arg = the_args[3]
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for query_datasets"
            print "query_datasets_annotation accepts four arguments"
            print "1) the catalog ID and 2) the field 3) the operator"
            print "4) the value"
            print "Example: python catalog.py query_datasets 17 name LIKE %New%"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False

        try:
            selector_list = [(field_arg,Op[operator_arg],value_arg)]
            _,result = wrap.catalogClient.get_datasets(catalog_arg, selector_list=selector_list)
        except KeyError:
            print 'Unknown query operator %s -- Known query Operators are'%operator_arg
            print Op.keys()
            return False

        if print_text is True:
            for dataset in result:
                print format_dataset_text(dataset)
            return True
        else:
            print json.dumps(result)
            return True

    elif(the_command == 'query_members'):
        catalog_arg = None
        dataset_arg = None
        field_arg = None
        operator_arg = None
        value_arg = None

        try:
            if default_catalog:
                catalog_arg = default_catalog
                dataset_arg = the_args[0]
                field_arg = the_args[1]
                operator_arg = the_args[2]
                value_arg = the_args[3]
            else:
                catalog_arg = the_args[0]
                dataset_arg = the_args[1]
                field_arg = the_args[2]
                operator_arg = the_args[3]
                value_arg = the_args[4]
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for query_members"
            print "query_members accepts five arguments"
            print "1) the catalog ID and 2)the dataset ID 3) the operator"
            print "3) the field 4) the operator and 5) the value"
            print "Example: python catalog.py query_members 17 84 name LIKE %New%"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False

        try:
            tmp_selector_list = [(field_arg,Op[operator_arg],value_arg)]
            print catalog_arg
            print dataset_arg
            print tmp_selector_list
            _,result = wrap.catalogClient.get_members(catalog_arg,dataset_arg,selector_list=tmp_selector_list)
        except KeyError:
            print 'Unknown query operator %s -- Known query Operators are'%operator_arg
            print Op.keys()
            return False

        if print_text is True:
            for dataset in result:
                print format_member_text(dataset)
                return True
        else:
            print json.dumps(result)
            return True

    elif(the_command == 'create_members'):
        catalog_arg = None
        dataset_arg = None
        member_arg = None

        try:
            if default_catalog:
                catalog_arg = default_catalog
                dataset_arg = the_args[0]
                member_arg = the_args[1]
            else:
                catalog_arg = the_args[0]
                dataset_arg = the_args[1]
                member_arg = the_args[2]
        except IndexError:
            print "==================ERROR===================="
            print "Invalid Arguments passed for create_members"
            print "create_members accepts three arguments"
            print "1) the catalog ID and 2)the dataset ID"
            print "3) the member JSON"
            print "==========================================="
            return False
        except KeyError, e:
            print 'KeyError:',e
            return False
        try:
            _,result = wrap.catalogClient.create_members(catalog_arg,dataset_arg,json.loads(member_arg))
            print "%s,%s,%s"%(catalog_arg,dataset_arg,result['id'])
        except Exception, e:
            print e
            return False
        return True


    elif(the_command == 'create_token_file'):
        wrap.create_token_file()
        return True
    elif(the_command == 'delete_token_file'):
        if(wrap.check_authenticate()):
            print '===Deleting access token==='
            wrap.delete_token_file()
        else:
            print 'No authentication token detected'

        return True

    else:
        print 'Invalid Command'
        return False

if __name__ == "__main__":

    command_list = ("get_catalogs","get_dataset_members","write_token",
                    "create_dataset","create_catalog","get_datasets",
                    "add_dataset_annotation","create_annotation_def",
                    "delete_catalog","delete_dataset","get_dataset_annotations","get_member_annotations",
                    "get_annotation_defs","test_command","get_datasets_by_name","query_datasets",
                    "add_member_annotation",'delete_token_file',"create_members", "query_members")
    flag_list = ("-text","-default_catalog")
    the_command = ''    #Stores the command to be executed via the catalogClient API
    selector_list = []
    the_flags = []      #Stores any flags detected in the arguments
    the_args = ''
    log_file = 'log/GlobusCatalog-log.txt'
    fail_log_file = 'log/GlobusCatalog-failed-log.txt'


    #Store authentication data in a local file
    token_file = os.getenv('HOME','')+"/.ssh/gotoken.txt"
    wrap = CatalogWrapper(token_file=token_file)

    #Check for any recognized flags from flag_list
    check_flags(flag_list,sys.argv)
 
    #Condition the input argument list. Slice at the first recognized command from command_list and set the_command
    the_args = split_args(command_list,sys.argv)

    #Execute the appropriate client action and check for appropriate args
    success = execute_command(the_command,the_args)

    arg_list = ['python']+sys.argv
    log_string = ' '.join(arg_list)

    with open(log_file, "a") as myfile:
        myfile.write(log_string+'\n')

    if success is False:
        with open(fail_log_file, "a") as myfile:
            myfile.write(log_string+'\n')

