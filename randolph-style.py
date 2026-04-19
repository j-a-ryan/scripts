import os
import pandas as pd
import numbers
import lookup
import utilities as util

########## Comment/uncomment to suit data ###############
R = 'randolph'# Two pairs of temp/leaf wetness
S = 'singleton' # One pair
SLG_T = 'singleton-low-gap-tables' # One pair, but with Low Gap naming
data_style = R 
# data_style = S
# data_style = SLG_T
#########################################################

## Some global constants
cleaned_file_name = 'cleaned.csv'
truncated_file_name = 'truncated.csv'
smoothed_file_name = 'smoothed.csv'
data_dir = '../'

latest_raw_data_dir = data_dir + 'latest/'
cleaned_file_path = data_dir + cleaned_file_name
truncated_file_path = data_dir + truncated_file_name
smoothed_file_path = data_dir + smoothed_file_name

columns_wanted = ''
if data_style == R:
    columns_wanted = lookup.columns_wanted_randolph_type_station
elif data_style == S:
    columns_wanted = lookup.columns_wanted_singleton_station
elif data_style == SLG_T:
    columns_wanted = lookup.columns_wanted_singleton_water_tables_low_gap_station
temp_1_field_name = ''
temp_2_field_name = ''
leaf_wetness_1_field_name = ''
leaf_wetness_2_field_name = ''
datetime_field_name = lookup.date_time_field_name
station_name = ''
transmitter_names = [] # Davis calls some of these 'stations'
temp_field_names = []
leaf_wetness_field_names = []
if len(columns_wanted) <= 2:
    for col in columns_wanted:
        if 'Date' in col:
            datetime_field_name = col
        elif 'Temp' in col:
            temp_field_name = col
            temp_field_names.append(temp_field_name)
        elif 'Leaf' in col:
            leaf_wetness_field_name = col
            leaf_wetness_field_names.append(leaf_wetness_field_name)
else:
    for col in columns_wanted:
        if 'Date' in col:
            datetime_field_name = col
        elif 'Temp' in col and '1' in col:
            temp_1_field_name = col
            temp_field_names.append(col)
        elif 'Temp' in col and '2' in col:
            temp_2_field_name = col
            temp_field_names.append(col)
        elif 'Leaf' in col and '1' in col:
            leaf_wetness_1_field_name = col
            leaf_wetness_field_names.append(col)
        elif 'Leaf' in col and '2' in col:
            leaf_wetness_2_field_name = col
            leaf_wetness_field_names.append(col)

#################### 1. The raw file ##########################################################
# Put the file in latest_data_dir. It should be the only file in there
# or the first file in there, according to os.listdir
# Open file, get station name from it.
print("Looking for raw data file....")
files = os.listdir(latest_raw_data_dir)
file_name = files[0]
print("Found the file: " + file_name)
full_file_path = latest_raw_data_dir + file_name
print("Opening file to find station name and transmitter names....")
with open(full_file_path, mode='r') as raw_file:
    print(raw_file.readline())
    station_name = raw_file.readline().strip('\n').strip('"').strip()  # Read station name from file.
    raw_transmitter_names_str = raw_file.readlines()[1].strip('\n').strip().replace('"', '') # A line has been read; the new line 1 should have the transmitter names.
    if raw_transmitter_names_str.startswith(','):
        raw_transmitter_names_str = raw_transmitter_names_str[1:]    
    transmitter_names = list(set(raw_transmitter_names_str.split(',')))
    
print("File contents show:")
print('\tstation name: ' + station_name)
print("\ttransmitter name(s): ")
for name in transmitter_names:
    print('\t\t' + name)          
########################## 2. Clean the data, creating cleaned.csv #############################
print("Opening file again for data cleaning, creating cleaned file called " + cleaned_file_name + "....")
util.clean(raw_file_path=full_file_path, cleaned_file_path=cleaned_file_path)
        
########################## 3. Eliminate unwanted columns #######################################
# Next get a convenient truncated file with only the data we need, using the cleaned file.
util.truncate(cleaned_file_path=cleaned_file_path, truncated_file_path=truncated_file_path)

########################## 4. Smooth the data to hourly ########################################
# Loop through the rows, smoothing the data to hourly from every 15 min : take mean of each four rows for each field
truncated_file_df = pd.read_csv(truncated_file_path)  # Get the truncated data from the truncated file.
smoothed_df = util.smooth(truncated_file_df=truncated_file_df, # Create a dataframe that contains the smoothed data.
                          temp_field_names=temp_field_names, 
                          leaf_wetness_field_names=leaf_wetness_field_names)


# Write the smoothed file
print("Now creating smoothed file....")
smoothed_df.to_csv(smoothed_file_path)
print("Created smoothed.csv")

########################## 5. Analyze the data #################################################
# Analyze the smoothed data
analysis_df = pd.DataFrame(smoothed_df)  # Because we'd like a new variable
smoothed_df = None
nonzero_risk_periods = []  # All the risk periods for this file, including all lw-temp sensor pairs.


# Loop through the analysis, using lookup to add risk units (if > 0) to risk_units_1_vals and risk_units_2_vals
# If a row has zero units to add to a tally and the tally > 0, then add to dictionary, key: start time, val: [ru, end time]
for i in range(len(temp_field_names)): # assumes lw and temp name arrays same length
    nonzero_risk_period_for_this_lw_temp_pair = util.analyze(analysis_df=analysis_df, leaf_wetness_field_name=leaf_wetness_field_names[i], temp_field_name=temp_field_names[i])
    nonzero_risk_periods.append(nonzero_risk_period_for_this_lw_temp_pair)

########################## 6. Report results of analysis #######################################

print("\n\n##################")
print("Results of analysis for " + station_name + ":")
rankings = []
for i in range(len(nonzero_risk_periods)):
    nonzero_risk_period = nonzero_risk_periods[i]
    if len(nonzero_risk_period) > 0:
        print(f"For sensor pair {i + 1}, we have these risk periods:")
        util.printout(nonzero_risk_period, rankings)
        print("...")
    else:
        print(f"There were no risk periods for sensor pair {i}.\n")
        
highest_rank_wet = 0
highest_rank_with_dry_time = 0
highest_rank_with_half_dry_time = 0
for ranking in rankings:
    if ranking[0] > highest_rank_wet:
        highest_rank_wet = ranking[0]
    if ranking[1] > highest_rank_with_dry_time:
        highest_rank_with_dry_time = ranking[1]
    if ranking[1] > highest_rank_with_half_dry_time:
        highest_rank_with_half_dry_time = ranking[1]
print("Risk level for wet time: " + util.get_risk_level(highest_rank_wet)[0])
print("Risk level incl full dry time: " + util.get_risk_level(highest_rank_with_dry_time)[0])
print("Risk level incl half dry time: " + util.get_risk_level(highest_rank_with_half_dry_time)[0])


# Next use the data again to find the total number of risk units, counting all and only
# risk periods that were equal to or above the infection threshold of the model (157 ru, could almost
# hard-code that in). Report the total.
net_all_sensor_pairs_infection_period_ru = 0
net_all_sensor_pairs_infection_period_ru_wet = 0
net_all_sensor_pairs_infection_period_ru_avg = 0
for i in range(len(nonzero_risk_periods)):
    infection_period_ru = 0
    infection_period_ru_wet = 0
    infection_period_ru_avg = 0
    nonzero_risk_period = nonzero_risk_periods[i]
    if len(nonzero_risk_period) > 0:
        for key, value in nonzero_risk_period.items():
            total_ru = value[0]
            dry_ru = value[1]
            end_time = value[2]
            wet_ru = total_ru - dry_ru
            avg_ru = (total_ru + wet_ru) / 2
            if total_ru > lookup.min_ru_for_infection:
                infection_period_ru += total_ru
            if wet_ru > lookup.min_ru_for_infection:
                infection_period_ru_wet += wet_ru
            if avg_ru > lookup.min_ru_for_infection:
                infection_period_ru_avg += avg_ru
        print(f"\n\nTotal RUs for sensor pair {i + 1}:")
        print("Total (incl dry-off) inf period RU: " + str(infection_period_ru))
        print("Wet-only (no dry-off) inf period RU: " + str(infection_period_ru_wet))
        print("Avg (allowing half dry-off time) inf period RU: " + str(infection_period_ru_avg))
        if infection_period_ru > net_all_sensor_pairs_infection_period_ru:
            net_all_sensor_pairs_infection_period_ru = infection_period_ru
        if infection_period_ru_wet > net_all_sensor_pairs_infection_period_ru_wet:
            net_all_sensor_pairs_infection_period_ru_wet = infection_period_ru_wet
        if infection_period_ru_avg > net_all_sensor_pairs_infection_period_ru_avg:
            net_all_sensor_pairs_infection_period_ru_avg = infection_period_ru_avg
        

print(f"\n\nNet for all sensor pairs total RUs:")
print("Total (incl dry-off) inf period RU: " + str(net_all_sensor_pairs_infection_period_ru))
print("Wet-only (no dry-off) inf period RU: " + str(net_all_sensor_pairs_infection_period_ru_wet))
print("     Avg (allowing half dry-off time) inf period RU: " + str(net_all_sensor_pairs_infection_period_ru_avg))