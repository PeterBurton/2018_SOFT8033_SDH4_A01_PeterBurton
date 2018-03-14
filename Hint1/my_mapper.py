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
# FUNCTION strip_project
# ------------------------------------------
def strip_project(s):
    return s[:2]

# ------------------------------------------
# FUNCTION strip_comma
# ------------------------------------------
def strip_comma(s):
    s = s.replace(',', '')
    return s
# ------------------------------------------
# FUNCTION my_map
# ------------------------------------------
def my_map(input_stream, languages, num_top_entries, output_stream):
    
    # Create lists to hold page language, title and views seperately
    page_lang = []
    page_title = []
    page_views = []
    
    # Process lines in the input stream
    for line in input_stream:
        
        #split the line into it's constituent words
        words = line.split(' ')
        #Extract language title and views
        language = words[0]
        title = words[1]
        #Strip comma from title (Getting a problem with certain entries e.g. I_,Tonya)
        title = strip_comma(title)
        views = words[2]
        #Strip the project from the language
        language_stripped = strip_project(language)
        
        for lang in languages:
            
            if lang == language_stripped:
                #If the language is English, French or Spanish add the entry to the lists
                page_lang.append(language)
                page_title.append(title)
                page_views.append(views)
    
    #Create a list to hold results
    results = []
    
    #Amalgamate the lists into one list of results
    for i in range(0, len(page_lang)):
        result_string = page_lang[i] +  ' ' + page_title[i] + ' ' + page_views[i] + '\n'
        results.append(result_string)
        
    #sort the results by language and project    
    results.sort()
    
    #current_lang variable to keep track of the language as I work through the results list
    wordlist = results[0].split(' ')
    current_lang = wordlist[0]
    
    #current_lang_proj is a list to hold all entries of the current language and project
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
            current_lang_proj.append([language, title, views])
            
        else:
            #This means there is no more of the previous language so now we sort, 
            #get the top n entries and write to file
            current_lang_proj.sort(key=lambda x: int(x[2]), reverse = True)
            top_results = current_lang_proj[:num_top_entries]
            
            for line in top_results:
                result_string = line[0] + "\t(" + line[1] + "," + line[2] + ")\n"
                output_stream.write(result_string)
            
            #Empty the list and start to fill it again with the new language
            current_lang_proj = []
            current_lang_proj.append([language, title, views])
            
            #Set current_lang = to language
            current_lang = language
        
                
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(debug, i_file_name, o_file_name, languages, num_top_entries):
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
    my_map(my_input_stream, languages, num_top_entries, my_output_stream)

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

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    # 2. Call to the function
    my_main(debug, i_file_name, o_file_name, languages, num_top_entries)
