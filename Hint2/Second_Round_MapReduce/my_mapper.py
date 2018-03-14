#!/usr/bin/python

# --------------------------------------------------------
#           PYTHON PROGRAM
# Here is where we are going to define our set of...
# - Imports
# - Global Variables
# - Functions
# ...to achieve the functionality required.
# When executing > python 'this_file'.py in a terminal,
# the Python interpreter will load our program,
# but it will execute nothing yet.
# --------------------------------------------------------

import sys
import codecs

# ------------------------------------------
# FUNCTION strip_comma
# ------------------------------------------
def strip_comma(s):
    s = s.replace(',', '')
    return s

#---------------------------------------
#  FUNCTION strip_line
#---------------------------------------
def strip_line(line):
    # Get rid of newline character
    line = line.replace('\n', '')

    # Split the string by the tabulator delimiter
    words = line.split(' ')

    # Get the key and the value and return them
    language = words[0]
    title = words[1]
    views = words[2]

    return language, title, views

# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, per_language_or_project, output_stream):
    
    #create list to hold all the input from the input stream
    input_list = []
    
    # Process lines in the input stream
    for line in input_stream:
        input_list.append(line)
    
    # create result list to hold modified input_list with changed language or project
    results = []
    
    for line in input_list:
        
        if per_language_or_project:
            
            #Separate the language, title and views into variables
            (language, title, views) = strip_line(line)
            language = language.split(".")[0]
            results.append(language + " " + title + " " + views + "\n")
        
        else:
            #Separate the project, title and views into variables
            (language, title, views) = strip_line(line)
            if '.' in language:
                project = language.split('.',1)[-1]
                if '.' in project:
                    #print(project)
                    project = project.split('.')[0]
                    #print(project)
                    
                results.append(project + " " + title + " " + views + "\n")
                #print(project + " " + title + " " + views + "\n")
            else:
                project = 'wikipedia'
                results.append(project + " " + title + " " + views + "\n")
                #print(project + " " + title + " " + views + "\n")
            
               
            
    #Sort the results list on language/project
    results.sort()
    
    #current_lang variable to keep track of the language as I work through the results list
    wordlist = results[0].split(' ')
    current_lang = wordlist[0]
    
    #current_lang_proj is a list to hold all entries of the current language or project
    current_lang_proj = []
    
    for line in results:
        #split the line into it's constituent words
        words = line.split(' ')
        #Extract language title and views
        language = words[0]
        title = words[1]
        views = words[2]
        #strip the newline character from views
        views = views.replace('\n', '')
        
        if language == current_lang:
            current_lang_proj.append(int(views))
            
        else:
            #This means there is no more of the previous language so now we 
            #add up all the views and write to file
            
            #petitions is number of petitions for that language
            petitions = 0
            
            for i in current_lang_proj:
                petitions = petitions + i
            
            result_string = current_lang + "\t" + str(petitions) + "\n"
            output_stream.write(result_string)
                
            #Empty the list and start to fill it again with the new language
            current_lang_proj = []
            current_lang_proj.append(int(views))
            
            #Set current_lang = to language
            current_lang = language
    
    #I had to put this block of code in as my loops weren't accounting for the last 
    #project/language
    petitions = 0
    for i in current_lang_proj:
        petitions = petitions + i
    result_string = current_lang + "\t" + str(petitions) + "\n"
    output_stream.write(result_string)
    
    
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, per_language_or_project):
    # We pick the working mode:

    # Mode 1: Debug --> We pick a file to read test the program on it
    if debug == True:
        my_input_stream = codecs.open(i_file_name, "r", encoding='utf-8')
        my_output_stream = codecs.open(o_file_name, "w", encoding='utf-8')
    # Mode 2: Actual MapReduce --> We pick std.stdin and std.stdout
    else:
        my_input_stream = sys.stdin
        my_output_stream = sys.stdout

    # We launch the Map program
    my_map(my_input_stream, per_language_or_project, my_output_stream)

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    # 1. Input parameters
    debug = True

    i_file_name = "pageviews-20180219-100000_0.txt"
    o_file_name = "mapResult.txt"

    per_language_or_project = False # True for language and False for project

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, per_language_or_project)
