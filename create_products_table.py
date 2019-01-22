"""
1. Sequential execution of tasks
2. Directed graph
3. Parallel execution paths
4. Parallel execution
"""
from __future__ import print_function
from collections import OrderedDict
from dask.multiprocessing import get

import logging
import time
import os
import re
import threading

def find_available_tables():
    #Return the list of available raw tables
    raw_tables_list = []
    files_list = os.listdir('raw')
    for file_name in files_list:
        raw_tables_list.append("raw." + file_name)
    return raw_tables_list

def find_dependencies(raw_tables_list):
    #Parse all the files in tmp and find dependencies between all the sql scripts
    table_name_pattern = re.compile('`[^`]*`')
    tmp_files = os.listdir('tmp')
    processed_tables = raw_tables_list
    dependency_level = {}
    level_count = 0;
    print("\nSIMPLE VISUALIZATION OF DEPENDENCIES")
    while tmp_files:
        #process the files and remove from list
        print("\nITERATION {}\n".format(level_count+1))
        level_processed_tables = []
        level_count = level_count + 1
        for filename in tmp_files:
            f = open((os.path.join('tmp', filename)), "r")
            query = f.read()
            f.close
            #Extract the list of tables used in the query
            tables_used = []
            for table_name in table_name_pattern.findall(query):
                tables_used.append(table_name[1:-1])
            tmp_table_name, ext = filename.split(".")
            if all(elem in processed_tables  for elem in tables_used):
                level_processed_tables.append('tmp.'+tmp_table_name)
                if level_count in dependency_level:
                    dependency_level[level_count].append(r'{}\{}'.format('tmp', filename))
                else:
                    dependency_level[level_count] = [r'{}\{}'.format('tmp', filename)]
                #display the dependency
                for t1 in tables_used:
                    print(t1 + " -", end=" ")
                print("------> tmp.{}".format(tmp_table_name))
        #add to all processed tables - processed_tables + level_processed_tables
        processed_tables = processed_tables + level_processed_tables
        #remove level processed tables from unprocessed tmp files
        for lpt in level_processed_tables:
            fold, t_name = lpt.split(".")
            f_name = t_name + ".sql"
            tmp_files.remove(f_name)
    #Add files in final/ as last level files
    final_files = os.listdir('final')
    print("\nITERATION {}\n".format(level_count+1))
    for final_file in final_files:
        if level_count+1 in dependency_level:
            dependency_level[level_count+1].append("final\\"+final_file)
        else:
            dependency_level[level_count+1] = ["final\\"+final_file]
        #display the dependency
        #Extract the list of tables used in the query
        f = open((os.path.join('final', final_file)), "r")
        query = f.read()
        f.close
        tables_used = []
        for table_name in table_name_pattern.findall(query):
            tables_used.append(table_name[1:-1])
        final_table_name, ext = final_file.split(".")
        for t1 in tables_used:
            print(t1 + " -", end=" ")
        print("------> final.{}".format(final_table_name))
    return dependency_level

def find_directed_graph(raw_tables_list):
    #Parse all the files in tmp and find directed graph
    table_name_pattern = re.compile('`[^`]*`')
    tmp_files = os.listdir('tmp')
    processed_tables = raw_tables_list
    directed_graph = {}
    level_count = 0;
    while tmp_files:
        #process the files and remove from list
        level_processed_tables = []
        level_count = level_count + 1
        for filename in tmp_files:
            f = open((os.path.join('tmp', filename)), "r")
            query = f.read()
            f.close
            #Extract the list of tables used in the query
            tables_used = []
            for table_name in table_name_pattern.findall(query):
                tables_used.append(table_name[1:-1])
            tmp_table_name, ext = filename.split(".")
            if all(elem in processed_tables  for elem in tables_used):
                level_processed_tables.append('tmp.'+tmp_table_name)
                #Build the directed graph
                for t1 in tables_used:
                    if t1 in directed_graph:
                        directed_graph[t1].append("tmp."+tmp_table_name)
                    else:
                        directed_graph[t1] = ["tmp."+tmp_table_name]
        #add to all processed tables - processed_tables + level_processed_tables
        processed_tables = processed_tables + level_processed_tables
        #remove level processed tables from unprocessed tmp files
        for lpt in level_processed_tables:
            fold, t_name = lpt.split(".")
            f_name = t_name + ".sql"
            tmp_files.remove(f_name)
    #Add final file as last node to the graph
    final_files = os.listdir('final')
    for final_filename in final_files:
        final_file_read = open((os.path.join('final', final_filename)), "r")
        final_query = final_file_read.read()
        final_file_read.close
        tables_used_in_final = []
        for table_name in table_name_pattern.findall(final_query):
            tables_used_in_final.append(table_name[1:-1])
        final_table_name, ext = final_filename.split(".")
        for tf in tables_used_in_final:
            if tf in directed_graph:
                directed_graph[tf].append("final."+final_table_name)
            else:
                directed_graph[tf] = ["final."+final_table_name]
    return directed_graph

def find_shortest_path(graph, start, end, path=[]):
    """
       Returns the shortest path from start to end
    """
    path = path + [start]
    if start == end:
        return path
    if not graph.has_key(start):
        return None
    shortest = None
    for node in graph[start]:
        if node not in path:
            newpath = find_shortest_path(graph, node, end, path)
            if newpath:
                if not shortest or len(newpath) < len(shortest):
                    shortest = newpath
    return shortest

def find_parallel_execution(directed_graph):
    tmp_files = os.listdir('tmp')
    final_files = os.listdir('final')
    execution_paths = []
    for start_node in tmp_files:
        for end_node in final_files:
            #Find the shortest path from tmp file to final file
            path = find_shortest_path(directed_graph, ("tmp."+start_node[:-4]), ("final."+end_node[:-4]))
            execution_paths.append(path)
    return execution_paths

def execute_sql_file(file_name):
    #dummy function for running sql scripts
    print("Executing {}........".format(file_name))
    time.sleep(3)
   #(file_name[:-4]).replace("\\",".")
    print("Processed table {}".format((file_name[:-4]).replace("\\",".")))
    return file_name

def execute_dep_sql_file(file_name_list):
    #dummy function for running dependent sql scripts
    backtrack_mapping = {
        'tmp.variants': ['tmp.inventory_items', 'tmp.variant_images', 'tmp.item_purchase_prices'], 
        'tmp.products': ['tmp.product_images', 'tmp.product_categories'], 
        'final.products': ['tmp.variants', 'tmp.products']
    }
    new_list = []
    for f in file_name_list:
        new_name = (f[:-4]).replace("\\",".")
        new_list.append(new_name)
    #find match in backtrack mapping
    for k,v in backtrack_mapping.iteritems():
        if len(v)==len(new_list) and all(elem in v  for elem in new_list):
            file_to_be_executed = k
            break
    file_to_be_executed = (file_to_be_executed.replace(".","\\"))+".sql"
    print("Executing {}........".format(file_to_be_executed))
    time.sleep(3)
    print("Processed table {}".format((file_to_be_executed[:-4]).replace("\\",".")))
    return file_to_be_executed

def run_parallel_execution(dependency_level, backtrack_mapping):
    #Construct dask graph for parallel execution, using the backtrack mapping
    dsk = OrderedDict()
    #Execute sql files in level 1 of dependency level
    first_level = min(k for k, v in dependency_level.iteritems())
    max_level = max(k for k, v in dependency_level.iteritems())
    first_level_files = dependency_level[first_level]
    index = 0
    first_level_files_mapping = {}
    for item in first_level_files:
        dsk["execute_sql_file"+str(index)] = (execute_sql_file, item)
        first_level_files_mapping[item] = "execute_sql_file"+str(index)
        index = index + 1

    #Add dependent tasks to dask graph
    index = 0
    for i in xrange(2, max_level+1):
        level_files = dependency_level[i]
        for item in level_files:
            #fetch dependent items of file from backtrack mapping
            table_item = (item[:-4]).replace("\\",".")
            dependent_items = backtrack_mapping[table_item]
            mapped_tasks = []
            for ditem in dependent_items:
                mapped_tasks.append(first_level_files_mapping[((ditem.replace(".","\\"))+".sql")])
            dsk["upstream_dependency"+str(index)] = (execute_dep_sql_file, mapped_tasks)
            first_level_files_mapping[item] = "upstream_dependency"+str(index)
            index = index + 1
    print("DASK GRAPH\n")
    for k,v in dsk.iteritems():
        print("{}  :  {}".format(k, v))
    #execute the tasks in parallel
    print("\nPARALLEL EXECUTION USING DASK\n")
    get(dsk, ('upstream_dependency'+str(index-1)))

def backtrack_execution_paths(execution_paths):
    """
       Construct backtrack mapping to map dependent tasks
    """
    backtrack_mapping = {}
    for path in execution_paths:
        size_of_path = len(path)
        while size_of_path>0:
            #add item to backtrack mapping
            size_of_path = size_of_path - 1
            if path[size_of_path] in backtrack_mapping:
                if size_of_path>0 and (path[size_of_path-1] not in backtrack_mapping[path[size_of_path]]):
                    backtrack_mapping[path[size_of_path]].append(path[size_of_path-1])
            else:
                if size_of_path>0:
                    backtrack_mapping[path[size_of_path]] = [path[size_of_path-1]]
    return backtrack_mapping

def run():
    #parse raw folder and find available tables
    raw_tables_list = find_available_tables()
    #parse files in tmp and find dependencies
    dependency_level = find_dependencies(raw_tables_list)
    #execute the files in order
    print("\nORDER OF EXECUTION OF SQL FILES\n")
    for iteration_level, execution_list in dependency_level.iteritems():
        for sql_file in execution_list:
            execute_sql_file(sql_file)
    #construct the directed graph
    directed_graph = find_directed_graph(raw_tables_list)
    print("\nDIRECTED GRAPH\n")
    for k,v in directed_graph.iteritems():
        print("{}  :  {}".format(k, v))
    #find all the shortest paths for parallel execution, using the directed graph
    print("\nPARALLEL EXECUTION PATHS\n")
    execution_paths = find_parallel_execution(directed_graph)
    for path in execution_paths:
        print(path)
    #Build backtracking representation for parallel execution of tasks, using the execution paths
    backtrack_mapping = backtrack_execution_paths(execution_paths)
    print("\nBACKTRACK MAPPING\n")
    for k,v in backtrack_mapping.iteritems():
        print("{}  :  {}".format(k, v))
    #execute tasks in parallel
    print("\nPARALLEL EXECUTION OF SCRIPTS\n")
    run_parallel_execution(dependency_level, backtrack_mapping)

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()