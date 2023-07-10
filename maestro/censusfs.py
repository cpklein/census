import os
import pytz
import datetime
#import re
import duckdb
import logging
from census_logging import * 

# Get the maestro logger
fsys_logger = logging.getLogger('maestro')

class FileSet:
    def __init__(self, file_dir, file_filter):
        self.conn = duckdb.connect(':default:')
        self.file_dir = file_dir
        self.tree = []
        self.filelist = []
        self.filelist_full = []
        self.filenames = []
        self.base_path = []
        self.recursive = True
        self.origin = []
        self.tags = []
        self.created_after = '-infinity'
        self.created_before = 'infinity'
        self.visibility = []
        self.status = []
        self.action = 'read'
        self.user = '_census_'
        if self.user == '':
            raise Exception('FileSet creation without user')
        self.groups = '_census_'
        if self.user == []:
            raise Exception('FileSet creation without groups')
        # In case we supply the filter, update it
        if file_filter:
            self.update_filter(file_filter)
        # Build the tree
        self.build_tree()
        # Build fset with all files under the tree
        self.build_fset()
        return

    def update_filter(self, file_filter):
        #Update filter values
        self.filenames = file_filter.get('filenames', self.filenames)
        self.base_path = file_filter.get('base_path', self.base_path)
        self.recursive = file_filter.get('recursive', self.recursive)
        self.origin = file_filter.get('origin', self.origin)
        self.tags = file_filter.get('tags', self.tags)
        self.created_after = file_filter.get('created_after', self.created_after)
        self.created_before = file_filter.get('created_before', self.created_before)
        self.visibility = file_filter.get('visibility', self.status)
        self.status = file_filter.get('status', self.status)
        self.action = file_filter.get('action', 'change')
        self.user = file_filter.get('user', '')
        if self.user == '':
            raise Exception('FileSet request without user')
        self.groups = file_filter.get('groups', [])
        if self.groups == []:
            raise Exception('FileSet request without groups')
        return
        
    def build_tree(self):
        # Retrieve all files from the base_path
        # If recurvise, look for all subdirectories
        if self.recursive:
            # build the base path from user
            path = ''
            for sub in self.base_path:
                path = os.path.join(path, sub)
            # call the recursive function that parses all subdirectories
            self.tree = self.__get_tree(os.path.join(self.file_dir, path))
    
    # Function to recurrently parse all subdirectories
    # Call only from withing the class
    def __get_tree(self, path):
        # Add base directory
        tree = [path]
        # Find subdirectories
        directories =  [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
        for directory in directories:
            # Call itself for the subdirectory
            tree = tree + self.__get_tree(os.path.join(path, directory))
        return tree

    # Insert all files into the fset database
    def build_fset(self):
        # Remove the previous table if existing
        try:
            self.conn.execute('DROP TABLE fset')
        except Exception as error:
            fsys_logger.debug(error)
        # Create table
        self.conn.execute("""CREATE TABLE fset(
                filename VARCHAR,
                path VARCHAR[],
                origin VARCHAR[],
                tags VARCHAR[],
                created TIMESTAMP,
                removed TIMESTAMP,
                hidden BOOLEAN,
                processed BOOLEAN,
                owner VARCHAR,
                changed_by VARCHAR,
                read_user VARCHAR[],
                read_group VARCHAR[],
                change_user VARCHAR[],
                change_group VARCHAR[])""")
        # Read .*.json from all directories
        for path in self.tree:
            # check if there are metadata files, otherwise break 
            #files = [f for f in os.listdir(path) if (os.path.isfile(os.path.join(path, f))
            #                                         and re.match(r'\B\..*\.json\Z', f))]
            req = path + "/.*.json"
            try:
                self.conn.execute("""INSERT INTO fset SELECT * FROM read_json($dir, auto_detect=true, union_by_name=true)""",
                            { "dir" : req }
                )
            except Exception as error:
                fsys_logger.debug(error)
                
    def get_files(self, file_filter):
        #Update filter values if filter is provided
        if file_filter:
            self.update_filter(file_filter)
        
        # Create a Relation with everything available
        flist_ref = self.conn.sql('SELECT * FROM fset')
        # Create an empty Relation
        flist_empty = self.conn.sql("SELECT * FROM flist_ref LIMIT 0")
        # Create the result flist
        flist = self.conn.sql("SELECT * FROM flist_ref LIMIT 0")

        # Filter on filenames
        if self.filenames:            
            # Create an empty Relation
            flist_tmp = self.conn.sql("select * from flist_ref limit 0")
            for filename in self.filenames:
                # Filter each filename
                flist_proc = flist_ref.filter ("filename = '{}'".format(filename))
                # Find what's new
                flist_new = flist_proc.except_(flist_tmp)
                # Add result to the cummulative list
                flist_tmp = flist_tmp.union(flist_new)
            # Update flist_ref
            flist = flist_empty.union(flist_tmp)
        else:
            flist = flist.union(flist_ref)

        # Filter on visibilty (no visibility means all)
        if self.visibility:
            # Create an empty Relation
            flist_tmp = self.conn.sql("select * from flist_ref limit 0")            
            # Filter on status hidden
            if 'hidden' in self.visibility:
                flist_tmp = flist.filter('hidden = true')
            # Filter on status unhidden
            if 'unhidden' in self.visibility:
                flist_proc = flist.filter('hidden = false')
                flist_tmp = flist_proc.union(flist_tmp)
            # Update flist
            flist = flist_empty.union(flist_tmp)

        # Filter on dates
        if self.created_after:
            flist = flist.filter("created > '{}'".format(self.created_after))
        if self.created_before:
            flist = flist.filter("created < '{}'".format(self.created_before))

        # Filter on processed status (no status means all)
        if self.status:
            # Create an empty Relation
            flist_tmp = self.conn.sql("select * from flist_ref limit 0")            
            # Filter on status processed
            if 'processed' in self.status:
                flist_tmp = flist.filter('processed = true')
            # Filter on status unprocessed
            if 'unprocessed' in self.status:
                flist_proc = flist.filter('processed = false')
                flist_tmp = flist_proc.union(flist_tmp)
            # Update flist
            flist = flist_empty.union(flist_tmp)
            
        # Filter on path
        path_base = ''
        for sub in self.base_path:
            path_base = os.path.join(path_base, sub)
        if path_base:
            # Just execute if there is a path to filter. Otherwise do nothing
            # Filter all files that concat path starts with path_base
            flist = flist.filter("starts_with(array_to_string(path, '/'), '{}')".format(path_base))

        # Read or Change
        if self.action == 'read':
            # Define Filter for the user
            filter_user = "list_contains(read_user, '{}')"
            # define filter for the group
            filter_group = "list_contains(read_group, '{}')"
        else:
            # Define Filter for the user
            filter_user = "list_contains(change_user, '{}')"
            # define filter for the group
            filter_group = "list_contains(change_group, '{}')"           
        # Apply user filter
        flist_tmp = flist.filter (filter_user.format(self.user))
        # The user may belong to multiple groups. Check each one individualy
        for u_group in self.groups:
            # Filter based on the group
            flist_group = flist.filter (filter_group.format(u_group))
            # Find what's new
            flist_new = flist_group.except_(flist_tmp)
            # Add result to the cummulative list
            flist_tmp = flist_tmp.union(flist_new)
        # Update flist
        flist = flist_empty.union(flist_tmp)
            
        # Filter on tags (no tags means all)
        if self.tags:            
            # Create an empty Relation
            flist_tmp = self.conn.sql("select * from flist_ref limit 0")
            for tag in self.tags:
                # Filter each tag
                flist_tag = flist.filter ("list_contains(tags, '{}')".format(tag))
                # Find what's new
                flist_new = flist_tag.except_(flist_tmp)
                # Add result to the cummulative list
                flist_tmp = flist_tmp.union(flist_new)
            # Update flist_ref
            flist = flist_empty.union(flist_tmp)

        # Filter on origin (no origin means all)
        if self.origin:            
            # Create an empty Relation
            flist_tmp = self.conn.sql("select * from flist_ref limit 0")
            for origin in self.origin:
                # Filter each origin
                flist_proc = flist.filter ("list_contains(origin, '{}')".format(origin))
                # Find what's new
                flist_new = flist_proc.except_(flist_tmp)
                # Add result to the cummulative list
                flist_tmp = flist_tmp.union(flist_new)
            # Update flist_ref
            flist = flist_empty.union(flist_tmp)
            
        #Build filelist
        flist_sql = self.conn.sql("select filename, array_to_string(path, '/') from flist")
        flist_list = flist_sql.fetchall()
        self.filelist = [ os.path.join(file_dir, path, file) for file, path in flist_list]

        #Build flist_full - array with files
        resp = flist.fetchall()
        columns = flist.columns
        self.filelist_full =  [dict(zip(columns,register)) for register in resp]
        return
    
class File:
    def __init__(self, file_dir, meta, origin):
        tz = pytz.timezone('Brazil/East')        
        self.meta = {
            'filename' : meta['filename'],
            'path' : meta['path'],
            'origin' : [origin],
            'tags' : meta['tags'],
            'created' : datetime.datetime.now(tz=tz).strftime('%Y-%m-%d %H:%M:%S.%f%Z'),
            'removed' : '2100-01-01 00:00:00.000000-00',
            'hidden' : False,
            'processed' : False,
            'owner' : meta['user'],
            "changed_by" : meta['user'],
            "read_user" : meta['read_user'],
            "read_group" : meta['read_group'],
            "change_user" : meta['change_user'],
            "change_group" : meta['change_group']            
        }
    
        #with open(os.path.join(file_dir, meta['local_path'], meta['filename']), 'w') as f_out:
            #f_out.write(data_str)    