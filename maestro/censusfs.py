import os
#import re
import duckdb
import logging
from census_logging import * 

# Get the maestro logger
fsys_logger = logging.getLogger('maestro')

class FileSet:
    def __init__(self, file_filter, file_dir):
        self.conn = duckdb.connect(':default:')
        self.file_dir = file_dir
        self.tree = []
        self.filelist = []
        self.filenames = file_filter.get('filenames', [])
        self.base_path = file_filter.get('base_path', [])
        self.recursive = file_filter.get('recursive', False)
        self.origin = file_filter.get('origin', [])
        self.tags = file_filter.get('tags', [])
        self.created_after = file_filter.get('created_after', '')
        self.created_before = file_filter.get('created_before', '')
        self.status = file_filter.get('status', [])
        self.user = file_filter.get('user', '')
        if self.user == '':
            raise Exception('FileSet creation without user')
        self.groups = file_filter.get('groups', [])
        if self.user == []:
            raise Exception('FileSet creation without groups')
        # When we receive the file list ready from the user
        if self.filenames != []:
            self.filelist = self.filenames
            return
        self.build_tree()
        self.build_fset()
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
            self.tree = self.get_tree(os.path.join(self.file_dir, path))
            
    def get_tree(self, path):
        # Add base directory
        tree = [path]
        # Find subdirectories
        directories =  [f for f in os.listdir(path) if os.path.isdir(os.path.join(path, f))]
        for directory in directories:
            # Call itself for the subdirectory
            tree = tree + self.get_tree(os.path.join(path, directory))
        return tree
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
                origin VARCHAR,
                tags VARCHAR[],
                created TIMESTAMP,
                removed TIMESTAMP,
                hidden BOOLEAN,
                processed BOOLEAN,
                user VARCHAR[],
                group_ VARCHAR[],
                read VARCHAR[],
                change VARCHAR[])""")
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
                
# select * from files where list_contains(tags, 'tag03');
        
        
        
    