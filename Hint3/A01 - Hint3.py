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

# ------------------------------------------
# FUNCTION strip_project
# ------------------------------------------
def strip_project(s):
    return s[:2]


# ------------------------------------------
# FUNCTION process_language
# ------------------------------------------
def process_language(line, languages):

    # Get rid of newline character
    line = line.replace('\n', '')

    # Split the line into it's constituent words
    words = line.split(' ')

    # Extract language title and views
    language = words[0]
    title = words[1]
    # Strip comma from title (Getting a problem with certain entries e.g. I_,Tonya)
    title = strip_comma(title)
    views = int(words[2])

    # Strip the project from the language to use for the if statement
    language_stripped = strip_project(language)

    for lang in languages:
        if lang == language_stripped:
          # Create the output variable
          res = ()
          res = (language, title, views)
          # We return res
          return res
    
# ------------------------------------------
# FUNCTION my_main
# ------------------------------------------
def my_main(dataset_dir, o_file_dir, languages, num_top_entries):
    # Remove the solution directory, to rewrite into it
    dbutils.fs.rm(o_file_dir, True)

	# Complete the Spark Job
    # Read the dataset into an RDD
    input_rdd = sc.textFile(dataset_dir)

    # Filter the desired lines, i.e. only ones with en, es or fr
    processed_rdd = input_rdd.map(lambda x: process_language(x, languages))
    
    # Filter out the 'None' values that got returned
    filtered_rdd = processed_rdd.filter(lambda x: x is not None)
    
    # Convert rdd to have language as key
    converted_rdd = filtered_rdd.map(lambda(language, title, views): (language, (title, views)))
    
    # Perform reduce operation to get all entries for one language together
    # For example (language, [(title, views),(title, views),(title, views),........,(title, views)])
    aggregated_rdd = converted_rdd.mapValues(lambda value: [value])
    rdd_after_reduceByKey = aggregated_rdd.reduceByKey(lambda a, b: a + b)
    
    # Sort the values by views and use a slice to limit it to top 5
    top_5_rdd = rdd_after_reduceByKey.map(lambda (k, v): (k, sorted(v, key=lambda x: x[1], reverse=True)[:num_top_entries]))
    
    # Write the rdd out to the file
    # Unfortunately, although the results are correct and in order, the formatting leaves a little to be desired.
    # Each line of the file is a language, like so: (language, [(title, views),(title, views),(title, views),(title, views),(title, views)]),
    # instead of the example result which has the 5 results per language over 5 lines. However, when you look at the example file
    # the results for each language are unordered, whereas my results are in order for each language.
    top_5_rdd.saveAsTextFile(o_file_dir)




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

    languages = ["en", "es", "fr"]
    num_top_entries = 5

    my_main(dataset_dir, o_file_dir, languages, num_top_entries)
