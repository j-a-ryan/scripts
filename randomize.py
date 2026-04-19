import os, random

## Put text file with names, one on each line, starting on line one,
# in the latest dir: latest_raw_data_dir (see below). Script will make 
# a CSV file of four columns, with header, for four randomized reps.
data_dir = 'c:/rand/'
latest_raw_data_dir = data_dir + 'latest/'
output_file_name = 'trials.csv'
output_file_path = data_dir + output_file_name


files = os.listdir(latest_raw_data_dir)
file_name = files[0]
print("Found the file: " + file_name + ' in ' + latest_raw_data_dir)
full_file_path = latest_raw_data_dir + file_name
print("Opening file....")
names = None
with open(full_file_path, mode='r') as raw_file:
    names = raw_file.readlines()
cleaned_names = []
for name in names:
    cleaned_names.append(name.strip())

# Make the randomized blocks
random.shuffle(cleaned_names)
block1 = cleaned_names.copy()
random.shuffle(cleaned_names)
block2 = cleaned_names.copy()
random.shuffle(cleaned_names)
block3 = cleaned_names.copy()
random.shuffle(cleaned_names)
block4 = cleaned_names.copy()

num_names = len(cleaned_names)

# Create the rows, including a header (comment out header part if not wanted)
rows = []
header = 'Rep 1,Rep 2,Rep 3,Rep 4' 
rows.append(header) # comment out this line if header unwanted
for i in range(num_names):
    rows.append(block1[i] + ',' + block2[i] + ',' + block3[i] + ',' + block4[i])

# Create CSV file
print("Writing file " + output_file_name + " to " + data_dir)
num_rows = len(rows) # Can't used num_names because you might have included header now.
with open(output_file_path,'w+') as file:
    for i in range(num_rows):
        file.write(rows[i])
        if i < num_rows:
            file.write('\n')