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
# FUNCTION get_petitions
# ------------------------------------------
def get_petitions(line):
    
    # Create output variable
    res = ()
    
    # Get rid of newline character
    line = line.replace('\n', '')

    # Split the line into it's constituent words
    words = line.split(' ')
    
    # Extract the number of views/petitions
    views = int(words[2])
    
    # Return res
    res = (views)
    return res

#---------------------------------------
#  FUNCTION strip_line
#---------------------------------------
def strip_line(line):
    # Get rid of newline character
    line = line.replace('\n', '')

    # Split the string by the tabulator delimiter
    words = line.split(' ')

    # Get the values and return them
    language = words[0]
    title = words[1]
    views = int(words[2])

    return language, title, views
  
# ------------------------------------------
# FUNCTION process_language
# ------------------------------------------
def process_language(line):
    
    # Separate the project, title and views into variables
    (language, title, views) = strip_line(line)
    # Strip the project from the language
    language = language.split(".")[0]
    return (language, views)

# ------------------------------------------
# FUNCTION process_project
# ------------------------------------------
def process_project(line):

    # Separate the project, title and views into variables
    (language, title, views) = strip_line(line)
    # Strip out the language and leave the project
    if '.' in language:
      project = language.split('.',1)[-1]
      if '.' in project:
        project = project.split('.')[0]
      return (project, views)
    else:
    # If the project is blank then it's Wikipedia
      project = 'wikipedia'
      return (project, views)
    
# ------------------------------------------
# FUNCTION get_percentage
# ------------------------------------------
def get_percentage(x, total_views):
    #work out the overall percentage and return
    language = x[0]
    views = x[1]
    view_float = float(views)
    total_views_float = float(total_views)
    percent = view_float/total_views_float*100
    percent = str(percent) + '%'
    return (language, views, percent)

    
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, per_language_or_project):
  
    # Remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

	# Complete the Spark Job
    # Read the dataset into an RDD
    input_rdd = sc.textFile(dataset_dir)
    
    # Persist the input_rdd as I'll want to use it again later after I work out the total
    input_rdd.persist()
    
    # All I'm interested in on this pass is the total pageviews, so go over the 
    # rdd and just return the pageviews from the function
    page_views_rdd = input_rdd.map(lambda x: get_petitions(x))
    
    # reduce operation to get the total page views
    total_views = page_views_rdd.reduce(lambda x,y : x + y)
    
    # If boolean indicates per language
    if per_language_or_project:
      #Filter the rdd to get rid of the projects from the language
      processed_rdd = input_rdd.map(lambda x: process_language(x))
      
    #Else if it indicates per project
    else:
      # Filter the rdd to get rid of the languages and just have the project
      processed_rdd = input_rdd.map(lambda x: process_project(x))
    
    # Perform reduce operation using combineByKey to get the sum of views for each language/project
    totals_rdd = processed_rdd.combineByKey(
    # start with the first views figure
    createCombiner=lambda first_views: first_views,
    # add a new views figure to the sum
    mergeValue=lambda sum_views, new_views: sum_views + new_views,
    # combine the sums of views
    mergeCombiners=lambda sum_views_1, sum_views_2: sum_views_1 + sum_views_2
    )
    
    # Final map to create percentages for every entry by calling the function get_percentage
    final_rdd = totals_rdd.map(lambda x: get_percentage(x, total_views))
    
    # Save the output to file
    final_rdd.saveAsTextFile(o_file_dir)
    
    

# ---------------------------------------------------------------
#           PYTHON EXECUTION
# This is the main entry point to the execution of our program.
# It provides a call to the 'main function' defined in our
# Python program, making the Python interpreter to trigger
# its execution.
# ---------------------------------------------------------------
if __name__ == '__main__':
    dataset_dir = "/FileStore/tables/A01_my_dataset/"
    o_file_dir = "/FileStore/tables/A01_my_result/"

    per_language_or_project = True  # True for language and False for project

    my_main(dataset_dir, o_file_dir, per_language_or_project)
