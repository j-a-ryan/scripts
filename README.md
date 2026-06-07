# Scripts for Saunders Genetics Inc.

These Python scripts process micrometeorological data obtained from Davis Instruments sensors: temperature and leaf wetness. The CSV files are read and the boxwood blight risk is assessed in weekly accrued risk units using the standard boxwood blight risk model. Some variation is allowed for locations that typically dry quickly in the wind after rain. There a 3-hour drying time is alloted, instead of the model's 6-hour drying time.

The randolph-style.py file is the runner. At the top you will see instructions to comment/uncomment to suit the CSV file.

### Davis Leaf Wetness Sensor
<img width="750" height="518" alt="image" src="https://github.com/user-attachments/assets/6db990b3-ce9d-4e11-9540-ae79bb4cff3d" />

### Davis Temperature Probe and its Radiation Shield
<img width="385" height="222" alt="image" src="https://github.com/user-attachments/assets/00293bc0-17bd-4a1f-a739-2f028264365c" /><img width="500" height="500" alt="image" src="https://github.com/user-attachments/assets/5aede435-d2f2-459c-9670-a7169cfd06d6" />
