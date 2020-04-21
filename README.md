# apex_results_mapping
#############################################################################
############################# INSTRUCTIONS TO RUN ###########################
#############################################################################

# Requirements: Place results_analysis.py in the working directory. THE ROOT 
#                 DIRECTORY PATH MUST NOT CONTAIN ANY SPACES. In this directory 
#                 the following must also be present:
#
# Files or directories (without extention are directories):    
# ----working directory
#               -------RESULTS
#                         --- *.csv (results file from the main code)
#               -------results analysis
#                         ---- * (generated results directory with csv file name)
#                               ---- * state_county directory
#                                    --- *.asc (results files will be generated)
#                                    --- *.tif (results files will be generated)
#
# Output explanation: all the model simulation results are updated from csv file
#                     to tif and asc files for mapping.
