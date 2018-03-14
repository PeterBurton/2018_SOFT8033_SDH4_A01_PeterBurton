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

#---------------------------------------
#  FUNCTION strip_line
#---------------------------------------
def strip_line(line):
    # Get rid of newline character
    line = line.replace('\n', '')

    # Split the string by the tabulator delimiter
    words = line.split('\t')

    # Get the key and the value and return them
    language = words[0]
    views = words[1]

    return language, views

# ------------------------------------------
# FUNCTION my_reduce
# ------------------------------------------
def my_reduce(input_stream, total_petitions, output_stream):
    
    #create list to hold all the input from the input stream
    input_list = []
    
    #read the input stream into the list
    for line in input_stream.readlines():
        input_list.append(line)
    
    #Sort the list on language/project
    input_list.sort()
    
    #current_lang variable to keep track of the language as I work through the input list
    wordlist = input_list[0].split('\t')
    current_lang = wordlist[0]
    
    #current_lang_proj is a list to hold all entries of the current language and project
    current_lang_proj = []
    
    #results is an array to hold all the information, i.e. percentages before writing to file
    results = []
    
    for line in input_list:
        
        #Separate the language, title and views into variables
        (language, views) = strip_line(line)
        
        if language == current_lang:
            current_lang_proj.append(int(views))
            
        else:
            #This means there is no more of the previous language so now we 
            #add up all the views and work out percentage
            
            #petitions is number of petitions for that language
            petitions = 0
            
            for i in current_lang_proj:
                petitions = petitions + i
            
            percentage = (petitions/total_petitions)*100
            
            results.append([current_lang, petitions, percentage])
                
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
    percentage = (petitions/total_petitions)*100     
    results.append([current_lang, petitions, percentage])
    
    
    #Sort the results and then write to file
    results.sort(key=lambda x: x[2], reverse = True)
    
    for line in results:
        result_string = line[0] + "\t(" + str(line[1]) + ", " + str(line[2]) + "%)\n"
        output_stream.write(result_string)
        

# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, total_petitions, o_file_name):
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
    my_reduce(my_input_stream, total_petitions, my_output_stream)

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

    # This variable must be computed in the first stage
    total_petitions = 21996631

    i_file_name = "sort_simulation.txt"
    o_file_name = "reduce_simulation.txt"

    my_main(debug, i_file_name, total_petitions, o_file_name)
