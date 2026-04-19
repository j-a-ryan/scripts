import lookup
import pandas as pd
import numbers

datetime_field_name = lookup.date_time_field_name
num_dry_hours_to_stop_cycle = lookup.hours_drying_time  # If this is set to zero, code will probably need updating


# Analyze the data for a given temp field name and leaf wetness field name. Find discrete risk periods (period of nonzero risk) 
# and calculate and assign risk unit totals for each period. Place results in dictionary, which has structure:
#           start time mapping to array of [ru total, final dry ru total, end time]
def analyze(analysis_df: pd.DataFrame, leaf_wetness_field_name, temp_field_name) -> dict:
    risk_units_running_total = 0

    # Loop through the analysis, using lookup to add risk units (if > 0) to risk_units_1_vals and risk_units_2_vals
    # If a row has zero units to add to a tally and the tally > 0, then add to dictionary, key: start time, val: [ru, end time]
    start_time = ''
    num_rows = len(analysis_df)
    num_dry_hours = 0
    num_final_dry_risk_units = 0
    nonzero_risk_periods_for_this_sensor_pair = {}
    # Each row is assumed to be one hour
    for i in range(num_rows):
        lw = int(analysis_df[leaf_wetness_field_name][i])
        temp = int(analysis_df[temp_field_name][i])
        datetime = analysis_df[datetime_field_name][i]
        if lw > 0:  # If we are not in a final drying period....
            num_dry_hours = 0  # ... then zero out these values.
            num_final_dry_risk_units = 0
        elif risk_units_running_total > 0:  # ...else if it's dry but we're still in an infection cycle....:
            num_dry_hours += 1 # Each row is one hour
        
        # If either leaf wetness is above zero or we're still in a dry part of an infection cycle, then update the corresponding risk unit total. 
        # Otherwise, if there have been sufficient hours of dry time to stop the infection, then the risk event is finished. Update the corresponding 
        # dictionary with its data and zero out its risk unit var. Start with sensor pair 1
        if lw > 0 or (risk_units_running_total > 0 and num_dry_hours < num_dry_hours_to_stop_cycle):
            if temp in lookup.risk_units_lookup:  # If it's not very cold or very hot (We are not considering heat as killing the fungus or stopping the infection)
                if (not start_time):  # If we need to grab the start date for a new non-zero row for our results
                    start_time = datetime
                ru = lookup.risk_units_lookup[temp]
                risk_units_running_total += ru
                if num_dry_hours > 0:  # If we're possibly in final drying....
                    num_final_dry_risk_units += ru  # Add to the final dry risk units total.
                if i == num_rows - 1:  # If at end of file, finish the risk period. It is continuing, but there is nothing we can do about that. Make a new, extended file that has the enire risk period in it.
                    nonzero_risk_periods_for_this_sensor_pair[start_time] = [round(risk_units_running_total), round(num_final_dry_risk_units), datetime]  # start time maps to array of ru, final dry ru, end time
        else:
            if num_dry_hours >= num_dry_hours_to_stop_cycle or i == num_rows - 1:  # If we have finished drying or finished the file.
                if risk_units_running_total > 0:
                    nonzero_risk_periods_for_this_sensor_pair[start_time] = [round(risk_units_running_total), round(num_final_dry_risk_units), datetime]  # start time maps to array of ru, final dry ru, end time
                
                # Reset the variables
                start_time = ''
                risk_units_running_total = 0
                num_dry_hours = 0
    return nonzero_risk_periods_for_this_sensor_pair


def smooth(truncated_file_df: pd.DataFrame, temp_field_names, leaf_wetness_field_names):
    # Loop through the rows. Grab date from first row of every group of four rows, keeping arrays for calculating means.
    new_row_dict = {}
    smoothed_df = pd.DataFrame
    # These arrays will hold the arrays(s) of four values for calculating means
    temps = []
    lws = []
    for name in temp_field_names:
        temps.append([])
    for name in leaf_wetness_field_names:
        lws.append([])
    
    for index, row in truncated_file_df.iterrows():
        
        # Check whether beginning a group of four records:
        if index % 4 == 0 or index == len(truncated_file_df) + 1:  # If at beginning/end of group of 4, including end of file (may be group of fewer than 4 which we'll count as a group)
            if index > 0: # If it's not the first row
                # Calculate means, Use them in new df row. The lengths of the lists should be four 
                # but may be smaller if there was dirty nonumeric data in this group of four. When
                # this script cleans all dirty data (see interpolate issue above) then the groups will always be 
                # groups of four (except perhaps the last group in the file.)
                for i, name in enumerate(temp_field_names):
                    if len(temps[i]) == 0:
                        mean_temp = -1
                        print("FOUND MISSING DATA IN TRUNCATED FILE AT LINE " + str(index))
                        print("Please clean source file by hand at that location and rerun")
                    else:
                        mean_temp = round(sum(temps[i]) / len(temps[i]))
                    new_row_dict[name] = mean_temp
                for i, name in enumerate(leaf_wetness_field_names):
                    if len(lws[i]) == 0:
                        mean_lw = 0
                    else:
                        mean_lw = round(sum(lws[i]) / len(lws[i]))
                    new_row_dict[name] = mean_lw
                
                if smoothed_df.empty:
                    smoothed_df = pd.DataFrame(new_row_dict, index=[0])
                else:
                    smoothed_df = smoothed_df._append(new_row_dict, ignore_index=True)
                
                # Clear the lists
                for i in range(len(temps)):
                    temps[i].clear()
                for i in range(len(lws)):
                    lws[i].clear()
                new_row_dict = {}  # Just because why not.
            
            # Start a new row. We are on the next row of data now.
            new_row_dict[datetime_field_name] = row[datetime_field_name]
        
        if index <= len(truncated_file_df):
            # We are working on a group of four. Increment the vars, checking for dirty values, such as "--"
            # N.B.: we are assuming that all numbers are ints.
            
            for i, name in enumerate(temp_field_names):
                if isinstance(row[name], numbers.Number) or (isinstance(row[name], str) and row[name].isnumeric()):
                    temps[i].append(int(row[name]))
            for i, name in enumerate(leaf_wetness_field_names):
                if isinstance(row[name], numbers.Number) or (isinstance(row[name], str) and row[name].isnumeric()):
                    lws[i].append(int(row[name]))
    return smoothed_df
            

def truncate(cleaned_file_path, truncated_file_path):
    print("Now creating truncated file....")
    cleaned_file_df = pd.read_csv(cleaned_file_path)  # assumes encoding='utf-8' from cleaned file, as above
    columns_to_remove = lookup.columns_unwanted_randolph_type_station
    for col in columns_to_remove:
        if col in cleaned_file_df.columns: # Some abbreviated raw files do not have the unwanted columns to begin with.
            cleaned_file_df.pop(col)
    cleaned_file_df.to_csv(truncated_file_path) #, index=False)  # Pandas creates an index column by default. We'll use it
    print('Created truncated file: truncated.csv')
    
def clean(raw_file_path, cleaned_file_path):
    dirty_lines_count = 0
    with open(raw_file_path, mode='r') as raw_file, open(cleaned_file_path, mode='w', encoding='utf-8') as cleaned_file:
        lines_without_header = raw_file.readlines()[5:]  # Skip the header. [5:] should start on the column names row.
        for line in lines_without_header:
            cleaned_line = line.replace('"', '').replace("'", '')
            if '--' in cleaned_line: # TODO: any other non-numeric strings possible?
                print("Found line with \"--\" in it")
                dirty_lines_count +=1
            cleaned_file.write(cleaned_line)

    print("Data has been cleaned, cleaned.csv created. Checking for additional issues (NaN, etc.) requiring additional cleaning....")
    if dirty_lines_count > 0:
        print("Found " + str(dirty_lines_count) + " lines with nonumeric strings in them.")
        # TODO: When df.interpolate() is fixed, perhaps uncomment and use this code?
        # print("Doing some additional cleaning due to " + str(dirty_lines_count) + " that have dirty data")
        # nan_values = ['--']
        # # Load cleaned.csv into df, set its -- values to average of row above and below, if numeric. Note failure where non-numeric. Request manual cleaning in that case.
        # cleaned_file_df = pd.read_csv(cleaned_file_path)
        # for nan_val in nan_values:
        #     cleaned_file_df = cleaned_file_df.replace(nan_val, np.nan)
        # for col in lookup.columns_wanted_randolph_type_station:
        #     newvals = cleaned_file_df[col].interpolate(method='linear', limit_direction ='both')
        #     cleaned_file_df[col] = newvals
        # # cleaned_file_df.interpolate(method='linear', inplace=True)
        # cleaned_file_df.to_csv(cleaned_file2_path)

        
def get_risk_level(ru: int):
    risk_pair = ["No Risk", 0]
    if ru > 400:
        risk_pair[0] = "Very High Risk"
        risk_pair[1] = 6
    elif ru > 250:
        risk_pair[0] = "High Risk"
        risk_pair[1] = 5
    elif ru > 160:
        risk_pair[0] = "Moderate Risk"
        risk_pair[1] = 4
    elif ru > 56:
        risk_pair[0] = "Low Risk"
        risk_pair[1] = 3
    elif ru > 0:
        risk_pair[0] = "Slight Risk"
        risk_pair[1] = 2
    return risk_pair

def printout(risk_dict, rankings):
    for key, value in risk_dict.items():
        total_ru = value[0]
        dry_ru = value[1]
        end_time = value[2]
        wet_ru = total_ru - dry_ru
        avg_ru = (total_ru + wet_ru) / 2
        print(f"From {key} to {end_time}: {wet_ru} - {total_ru} (wet ru - total ru), avg {avg_ru}")
        rankings.append([wet_ru, total_ru, avg_ru])