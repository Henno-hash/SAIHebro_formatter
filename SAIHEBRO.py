# SCRIPT: SAIHEBRO.py
# AUTHOR: Henrik Schmidt
# DATE: 13.03.2022
#==================================================================================================================
# This script contains functions to clean-up SAIHEBRO meteorogical parameters csv-files from redundant whitespaces 
# and merge different parameter-files stationwise. For this purpose csv-files and one or more text-files 
# are needed. The text-files should contain the station-numbers where each station should occupie an extra line.
# E.g.:
# P080
# P081
# ...
#==================================================================================================================
import pandas as pd
import os, re
#==================================================================================================================
# Function to extract all the station numbers as an array
def array_stations(path):
    file = open(path,'r') # Open the file
    array_stations = file.readlines() # Read out each line and place them in an array
    # Check for ',' in the array (E.g.: P080,TEMPE OR P080)
    test = re.search(',', array_stations[0])
    if test != None: # E.g.: P080,TEMPE in text-file
        for value, line in enumerate(array_stations,0):
            array_stations[value] = line.split(',') # Split the line by ','
            array_stations[value] = array_stations[value][0] # Just keep the first element
            # E.g.: Returns only P080
    else: # E.g.: P080 in text-file
        array_stations = [line.strip() for line in file.readlines()] # Keep the line and just erase whitespaces
    file = None # Close the file, so no read/write conflict occurs
    
    return array_stations # Return the array
#==================================================================================================================
# Function to clean the saihebro csv-files from redundant whitespaces
# BEFORE CLEANING E.g.: date     ;  measurment_a         ;  measurement_b        ;....
# AFTER CLEANING E.g.: date; measurment_a; measurement_b; ....
def clean_saihebro(file):
    with open(file, 'r+') as f:
        csv = f.read().replace('  ', '').replace(' ;', ';')
        f.seek(0)
        f.write(csv)
        f.truncate()
#==================================================================================================================
# Function to combine the parameters stationwise
def combine(array_stations,array_parameter,path):
    # Generate timespan as an array
    times = pd.date_range(start="1998-01-01",end="2021-12-31").to_list()
    # Iterate through the text-file stations
    for station in array_stations:
        # Initiate parameter array, all used parameters for the station will be stored here (used in file-naming)
        parameter_array = [] 
        # Initiate Pandas dataframe with timespan as first column (key for later join operations)
        df_main = pd.DataFrame(data=times,columns=['date'])
        for file in array_parameter:
            if file[1]==station:
                clean_saihebro(file[0]) # Call clean_saihebro function
                df = pd.read_csv(file[0], encoding = 'latin', sep = ';') # Read csv-file as dataframe
                # ------------------------------------------------------------------------------------------------
                # Dataframe cleaning via parameter:
                if file[2]=='TEMPE':
                    df.drop(['Fecha máximo','Fecha mínimo'], axis=1,inplace=True) # Drop redundant columns
                    df.rename(columns={"Máximo": "T_max","Mínimo": "T_min","Media": "T_mean", "fecha ": "date"}, inplace=True)
                    df['date'] = pd.to_datetime(df['date']) # Change date datatype to datetime
                    df['T_max'] = df['T_max'].str.replace(',', '.') # Change punctuation to international
                    df['T_min'] = df['T_min'].str.replace(',', '.') # Change punctuation to international
                    df['T_mean'] = df['T_mean'].str.replace(',', '.') # Change punctuation to international
                    df['T_max'] = df['T_max'].astype(float) # Change datatype to float
                    df['T_min'] = df['T_min'].astype(float) # Change datatype to float
                    df['T_mean'] = df['T_mean'].astype(float) # Change datatype to float
                # ------------------------------------------------------------------------------------------------
                if file[2]=='PACUM':
                    df.drop(['Fecha acumulado','Máximo','Fecha máximo'], axis=1,inplace=True) # Drop redundant columns
                    df.rename(columns={"Acumulado": "precipitation", "fecha ": "date"}, inplace=True) # Rename columns
                    df['date'] = pd.to_datetime(df['date']) # Change date datatype to datetime
                    df['precipitation'] = df['precipitation'].str.replace(',', '.') # Change punctuation to international
                    df['precipitation'] = df['precipitation'].astype(float) # Change datatype to float
                # ------------------------------------------------------------------------------------------------
                if file[2]=='QRIO1' or file[2]=='QRIO2':
                    df.drop(['Fecha máximo','Fecha mínimo'], axis=1,inplace=True) # Drop redundant columns
                    df.rename(columns={"Máximo": "discharge_max","Mínimo": "discharge_min","Media": "discharge_mean", "fecha ": "date"}, inplace=True) # Rename columns
                    df['date'] = pd.to_datetime(df['date']) # Change date datatype to datetime
                    df['discharge_max'] = df['discharge_max'].str.replace(',', '.') # Change punctuation to international
                    df['discharge_min'] = df['discharge_min'].str.replace(',', '.') # Change punctuation to international
                    df['discharge_mean'] = df['discharge_mean'].str.replace(',', '.') # Change punctuation to international
                    df['discharge_max'] = df['discharge_max'].astype(float) # Change datatype to float
                    df['discharge_min'] = df['discharge_min'].astype(float) # Change datatype to float
                    df['discharge_mean'] = df['discharge_mean'].astype(float) # Change datatype to float
                # ------------------------------------------------------------------------------------------------
                if file[2]=='HUMED':
                    df.drop(['Fecha máximo','Fecha mínimo'], axis=1,inplace=True) # Drop redundant columns
                    df.rename(columns={"Máximo": "rel.humidity_max","Mínimo": "rel.humidity_min","Media": "rel.humidity_mean", "fecha ": "date"}, inplace=True) # Rename columns
                    df['date'] = pd.to_datetime(df['date']) # Change date datatype to datetime
                    df['rel.humidity_max'] = df['rel.humidity_max'].str.replace(',', '.') # Change punctuation to international
                    df['rel.humidity_min'] = df['rel.humidity_min'].str.replace(',', '.') # Change punctuation to international
                    df['rel.humidity_mean'] = df['rel.humidity_mean'].str.replace(',', '.') # Change punctuation to international
                    df['rel.humidity_max'] = df['rel.humidity_max'].astype(float) # Change datatype to float
                    df['rel.humidity_min'] = df['rel.humidity_min'].astype(float) # Change datatype to float
                    df['rel.humidity_mean'] = df['rel.humidity_mean'].astype(float) # Change datatype to float
                # ------------------------------------------------------------------------------------------------
                if file[2]=='RADIA':
                    df.drop(['Fecha máximo'], axis=1,inplace=True) # Drop redundant columns
                    df.rename(columns={"Máximo": "radiation_max","Media": "radiation_mean", "fecha ": "date"}, inplace=True) # Rename columns
                    df['date'] = pd.to_datetime(df['date']) # Change date datatype to datetime
                    df['radiation_max'] = df['radiation_max'].str.replace(',', '.') # Change punctuation to international
                    df['radiation_mean'] = df['radiation_mean'].str.replace(',', '.') # Change punctuation to international
                    df['radiation_max'] = df['radiation_max'].astype(float) # Change datatype to float
                    df['radiation_mean'] = df['radiation_mean'].astype(float) # Change datatype to float
                # ------------------------------------------------------------------------------------------------
                if file[2]=='VVIEN':
                    df.rename(columns={"VALOR ": "windspeed", "FECHA": "date"}, inplace=True) # Rename columns
                    df['windspeed'] = df['windspeed'].str.replace(',', '.') # Change punctuation to international
                    df['windspeed'] = df['windspeed'].astype(float) # Change datatype to float
                    df = df[df['date']>='03/10/2021'] # Filter for dates later then 02.10.2021
                    df['date'] = pd.to_datetime(df['date']) # Change date datatype to datetime
                    df = df.resample('D', on='date').mean() # Daily aggregation 
                    df.reset_index(inplace=True) # Reset index so the date isn't the index
                # ------------------------------------------------------------------------------------------------
                parameter_array.append(file[2]) # Append parameter to parameter_array
                df_main = pd.merge(df_main, df, on='date', how='left') # Merge the dataframe to the mainframe
            else: continue # If not equal, ignore the station
        # ------------------------------------------------------------------------------------------------
        df_main.fillna(-9999, inplace=True) # Fill NaN-Values with -9999,0
        # Save the mainframe as csv-file
        df_main.to_csv(r'{}\{}_{}.csv'.format(path,station,'_'.join(parameter_array)), index=False)  
    
    return df_main
#==================================================================================================================
# Main
if __name__=='__main__':
    #==================================================================================================================
    # Insert your path to your text-files containing all station numbers
    txt_lists_path = r'E:\1_Data_process\4_timeseries\SAIHebro\Climate\txt'
    txt_lists = os.listdir(txt_lists_path) # List all subfiles in the path
    for i in range(len(txt_lists)):
        txt_lists[i] = txt_lists_path+'\\'+txt_lists[i] # Append the fullpath to txt_lists array
    # Call the array_stations function to extract all station numbers from one text-file and list all station numbers
    all_stations = array_stations(txt_lists[0])
    #==================================================================================================================
    # If your are using seperate text-files for each parameter configuration, you can seperate the arrays like this:
    # pacum_stations = array_stations(txt_lists[0]) # Keep in mind to adjust the txt.lists index 
    # pacum_tempe_stations = array_stations(txt_lists[1])
    # tempe_stations = array_stations(txt_lists[2])
    # ...
    #==================================================================================================================
    # Insert the path to the timeseries (.csv) data
    time_series_path = r'E:\1_Data_process\4_timeseries\SAIHebro\Combi\0_rawdata'
    time_series_list = os.listdir(time_series_path) # List all subfiles in the path
    # Append the fullpath to time_series_list array, add the station number 
    for i in range(len(time_series_list)):
        time_series_list[i] = [time_series_path+'\\'+time_series_list[i],time_series_list[i][23:27],time_series_list[i][30:35]]
    #==================================================================================================================
    # Check if the stations of the listed csv-files are equal to the stations listed in the text-files
    all_parameter = [] # Array to append the matching stations
    #==================================================================================================================
    # If your are using seperate text-files for each parameter configuration, you can seperate the arrays like this:
    # pacum = []
    # pacum_tempe = []
    # tempe = []
    # ...
    #==================================================================================================================
    for i in time_series_list: # Iterate through the listed csv-files
        if i[1] in all_stations: # Equal with text-file ?
            all_parameter.append(i) # If equal append them to all_paramter
    #==================================================================================================================
    output_path = 'E:\1_Data_process\4_timeseries\SAIHebro\Combi'
    df = combine(all_stations,all_parameter,output_path) # Call the combine function # Call the combine function
    #==================================================================================================================
    # If your are using seperate text-files for each parameter configuration, you can seperate the arrays like this:
    # df = combine (pacum_stations, pacum,path)
    # df = combine (pacum_tempe_stations, pacum-tempe,path)
    # ...
    #==================================================================================================================