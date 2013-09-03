import sys
import json

from CatalogWrapper import *

print_text = False 	#Variable used to decide whether output should be in JSON (False) or limited plain text (True)

catalog_list = ''	#need this?




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


def execute_commands(the_command, the_args):
	global wrap

	#Tested
	if(the_command=='get_catalogs'):
		#takes no args, returns all visible catalogs
		try:
			_,catalog_list = wrap.catalogClient.get_catalogs()
			if print_text == True:
				print "============================================================"
				print "*More detailed catalog information available in JSON format*"
				print 'ID) Catalog Name - [Owner] - Catalog Description'
				print "============================================================"

				for catalog in catalog_list:
					#print catalog
					catalog_description = ''
					catalog_owners = ''
					catalog_name = ''

					try:
						catalog_description = catalog['config']['description']
					except Exception, e:
						catalog_description = ''

					try:
						catalog_name = catalog['config']['name']
					except Exception, e:
						catalog_name = 'no catalog name'

					print "%s)\t%s - [%s] - %s"%(catalog['id'], catalog_name, catalog['config']['owner'], catalog_description)

			else:
				print json.dumps(catalog_list)

			
			return True
		except Exception, e:
			#print e
			return False
		else:
			return False
	
	#		
	#
	#Current functionality is turned off on the alpha server
	elif(the_command=='create_catalog'):
		try:
			arg_dict = json.loads(the_args[0])
			print "CREATE CATALOG - %s"%(arg_dict)
			catalog_list = wrap.catalogClient.create_catalog(arg_dict)
			#wrap.create_catalog(the_args[0])
			return catalog_list
		except Exception, e:
			print e
			return False
		else:
			return False

	#Tested	
	elif(the_command=='delete_catalog'):
		#@arg[0] = catalog ID -- INT
		#@arg[1] = verify -- True to verify deletion
		try:
			if(the_args[0] != '' and (the_args[1] == "True" or the_args[1]=="true" or the_args[1]==True)):
				print "DELETE CATALOG - Catalog ID:%s"%(the_args[0])
				wrap.catalogClient.delete_catalog(the_args[0])
				return True
		except Exception, e:
			print e
			return False
		else:
			return False

	#Tested
	elif(the_command=='create_dataset'):
		#@arg[0] = catalog_id --  INT
		#@arg[1] = annotation list -- text string '{"name":"New Dataset"}'
		try:
			if(the_args[0] != '' and the_args[1] != ''):
				print "CREATE DATASET - Catalog ID:%s Annotations:%s"%(the_args[0],the_args[1])
				wrap.catalogClient.create_dataset(the_args[0],json.loads(the_args[1]))
				return True
		except Exception, e:
			print 'Exception:',e
			return False
		else:
			return False

	#Tested
	elif(the_command=='get_datasets'):
		#@arg[0] = catalog_id --  INT
		try:
			if(the_args[0] != ''):
				_,cur_datasets= wrap.catalogClient.get_datasets(the_args[0])
				
				print "============================================================"
				print "*More detailed dataset information available in JSON format*"
				print "ID) Dataset Name  - [Owner] - <Datset Labels>"
				print "============================================================"

				if print_text == True:
					for dataset in cur_datasets:
						dataset_labels = ''
						try:
							dataset_labels = ','.join(dataset['label'])
						except:
							dataset_labels = 'no labels'

						try:
							dataset_name = dataset['name']
						except Exception, e:
							dataset_name = 'no dataset name'

						#print dataset
						print "%s) %s - [%s] - <%s>"%(dataset['id'],dataset_name,dataset['owner'],dataset_labels)
				else:
					print json.dumps(cur_datasets)

				return True
		except Exception, e:
			print e
			return False
		else:
			return False

	elif(the_command=='create_annotation_def'):
		#@arg[0] = catalog_id -- INT
		#@arg[1] = annotation name - string
		#@arg[2] = value_type 

		try:
			#def create_annotation_def(self, catalog_id, annotation_name,
            #                  value_type, multivalued=False, unique=False):
			print "CREATE ANNOTATION DEF - Catalog ID:%s  Name:%s  Type:%s"%(the_args[0],the_args[1],the_args[2])
			response = wrap.catalogClient.create_annotation_def(catalog_id=the_args[0], annotation_name=the_args[1], 
				                                                    value_type=the_args[2])
			#wrap.create_catalog(the_args[0])
			print response
			return response
		except Exception, e:
			print e
			return False
		else:
			return False

	elif(the_command=='get_dataset_annotations'):
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
			_,dataset_annotations = wrap.catalogClient.get_dataset_annotations(arg0,arg1)
		elif(arg0 != '' and arg1 == ''):
			_,dataset_annotations = wrap.catalogClient.get_dataset_annotations(arg0)
		else:
			print 'Invalid arguments passed'
			return False

		if print_text == True:
			for annotation in dataset_annotations:
				print annotation
		else:	
			print json.dumps(dataset_annotations)
		return True

	elif(the_command=='add_dataset_tag'):
		#@arg[0] = catalog_id -- INT
		#@arg[1] = dataset id -- INT
		#@arg[2] = annotation list -- text string '{new-attribute:value}'
		try:
			if(the_args[0] != '' and the_args[1] != '' and the_args[2] != ''):
				wrap.add_dataset_annotations(the_args[0],the_args[1],the_args[2])
				print "ADD DATASET TAG - Catalog ID:%s Dataset ID:%s Annotations:%s",(the_args[0],the_args[1].the_args[2])
				return True
		except Exception, e:
			print e
			return False
	
	elif(the_command=='delete_dataset'):
		#@arg[0] = catalog ID -- INT
		#@arg[1] = dataset ID -- INT
		#@arg[2] = verify -- True to verify deletion
		try:
			if(the_args[0] != '' and the_args[1] != '' and (the_args[2] == "True" or the_args[2]=="true" or the_args[2]==True)):
				print "DELETE DATASET - Catalog ID:%s Dataset ID: %s"%(the_args[0],the_args[1])
				wrap.catalogClient.delete_dataset(the_args[0],the_args[1])
				return True
		except Exception, e:
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
				if print_text == True:
					print "============================================================"
					print "*More detailed member information available in JSON format*"
					print 'ID) Reference Dataset - Member Type - URI'
					print "============================================================"
					for member in cur_members:
						member_references = ''
						try:
							member_references = ','.join(member['dataset_reference'])
						except:
							member_labels = 'no references'
						print "%s) ref:%s - %s - %s"%(member['id'], member_references,member['data_type'], member['data_uri'])
				else:
					print json.dumps(cur_members)
				return True
		except Exception, e:
			print e
			return False
		else:
			return False
	
	elif(the_command=='query'):
		print "QUERY: UNDER IMPLEMENTATION",the_args
		return True

	elif(the_command=='create_token_file'):
		wrap.create_token_file()
		return True

	else:
		return False


if __name__ == "__main__":

	command_list = ("get_catalogs","get_dataset_members","write_token",
			        "create_dataset","create_catalog","get_datasets",
			        "add_dataset_tag","create_annotation_def",
			        "delete_catalog","delete_dataset","get_dataset_annotations","get_member_annotations","query")

	flag_list = ("-text")

	the_command = ''	#Stores the command to be executed via the catalogClient API
	the_flags = []		#Stores any flags detected in the arguments
	the_args = ''

	

	#Store authentication data in a local file
	token_file = "gotoken.txt"
	wrap = CatalogWrapper(token="file", token_file=token_file)
	
	#Input authentication data with every call
	#wrap = CatalogWrapper()

	#
	# file = open('gotoken.txt', 'w')
	# if(file):
	# 	wrap = CatalogWrapper(token = )	
	# else:

	#Grab any recognized flags
	check_flags(flag_list,sys.argv)

	#Condition the input argument list (slice at the first recognized command and set the_command)
	the_args = split_args(command_list,sys.argv)

	#Execute the apprpriate client action and check for appropriate args
	execute_commands(the_command,the_args)

