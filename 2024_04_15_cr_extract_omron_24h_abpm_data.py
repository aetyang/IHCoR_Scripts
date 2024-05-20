
# this script does the following:
# 1. extracts metadata from Omron 24/7 24hr reports (these are in pdf format)
#2. extracts relevant columns from the raw data presented in tables within the pdf_file
# 3. assembles all of the raw data and metadata into one csv file in long format
# 4. Author: Anthony O. Etyang. Last update 20 May 2024
    # corrected bug that was leading to loss of some of the individual BP readings over 24 24hrs
    # added two columns to the output -SerialNumber and Day so that output matches that one from spacelabs machine


# import libraries that will be needed for the task
import os
import pandas as pd
import numpy as np
from tabula import read_pdf # for reading tables in pdfs
import fitz  # PyMuPDF for reading text in pdfs
import re # regular expressions
from datetime import datetime

# Set the path to the folder containing the omron_24_7_revised PDF files
pdf_folder_path = "/Users/aetyang/Documents/Work folders/IHCOR Africa/Data/24hABPM/omron_24_7_revised"

# record today's date to use in the output file name
today_date = datetime.now().strftime("%Y-%m-%d")
print (today_date)

# Create an empty list to store filenames of PDFs with less than 70% successful readings
poor_quality_reading_list = []

# Set the path for the output CSV file
file_name=f"omron_results_kenya_{today_date}.csv"
#file_name_test= f"omron_results_kenya_test.csv"
output_csv_path = os.path.join("/Users/aetyang/Documents/Work folders/IHCOR Africa/Data/24hABPM/output/",file_name)
print(output_csv_path)
# sort the pdf files by _name_
pdf_files = sorted([f for f in os.listdir(pdf_folder_path) if f.endswith(".pdf")])
print(pdf_files)

# Create an empty DataFrame to store the extracted data
combined_data = pd.DataFrame()

# Create an empty list to store filenames of PDFs where data extraction failed
raw_tables_missing_list = []
#print (pdf_files)
pdf_file = pdf_files[2] #test file for debugging
#print (pdf_file)

# loop through each PDF file in the folder
for pdf_file in pdf_files:
    if pdf_file.endswith(".pdf"):
        # Extract identifier from file name
        study_id = pdf_file[:7] # study_id is composed of the first 7 characters of the (test)file name
        print(study_id)

        # Construct the full path to the PDF file
        pdf_path = os.path.join(pdf_folder_path, pdf_file)
        print (pdf_path)
        try:
            # Get the number of pages in the PDF
            pdf_document = fitz.open(pdf_path)
            n_pages = pdf_document.page_count
            print(n_pages)

            # Check if the PDF has fewer than 3 pages. Raw tables usually start from P2 of the pdf
            if n_pages < 3:
                print(f"{study_id} has less than 3 pages-Probable error. Skipping...")
                raw_tables_missing_list.append(pdf_file)
                continue  # Skip to the next iteration of the loop

            # Extract text from the first page (dates and other metadata are on the first page)
            first_page_text = pdf_document[0].get_text()
            print(first_page_text)

            # Extract the following using regular expressions:
             #start and end dates
             # serial number of the device is not available from the cardiovision output-can be obtained from study questionnaire
             # number of successful readings
             # 24hr sbp, 24hr dbp as computed by the Omron 24/7 device (this is to allow comparison with calculations based on raw data)
            date_pattern = r'\b\d{1,2}/\d{1,2}/\d{4}\b'
            dates = re.findall(date_pattern, first_page_text)
            print (dates)
            start_date = dates[1]
            print (start_date)
            end_date = dates[-1]
            print (end_date)
            #serial_number_match = re.search(r'Serial number:\s*(\d+-\d{6})', first_page_text)
            #print (serial_number_match)

            start_time_pattern = r'(\d{1,2}:\d{2} - (\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}))'
            start_times = re.findall(start_time_pattern,first_page_text)
            print (start_times)
            start_time_match = re.search(start_time_pattern,first_page_text)
            print (start_time_match)
            start_time= start_time_match.group(1)
            print (start_time)
            print (type(start_time))
            start_time= start_time[:5]
            start_time= start_time.replace(':','')
            start_time = int(start_time)
            type(start_time)
            print (start_time)

            end_time = start_time_match.group(2)
            print (end_time)
            end_time= end_time[11:16]
            print (end_time)
            end_time= end_time.replace(':','')
            end_time = int(end_time)
            print (end_time)

            #successful_readings_pattern = r'readings \s+(\d+)'
            #successful_readings_match = re.search(successful_readings_pattern, first_page_text)
            #print (successful_readings_match)


            successful_percentage_pattern = r'success ratio %\s+(\d+)'
            successful_percentage_match = re.search(successful_percentage_pattern, first_page_text)
            print (successful_percentage_match)
            success_percent = successful_percentage_match.group(1)
            print (success_percent)


            avg_bp_pattern= r'SBP/DBP weighted average\s+(\d+/\d+)\s+(-/-)\s+(\d+/\d+)\s+(\d+/\d+)'
            print(avg_bp_pattern)
            avg_bp_match=re.search(avg_bp_pattern, first_page_text)
            print(avg_bp_match)
            sbp_dbp_24h=avg_bp_match.group(1)
            print (sbp_dbp_24h)
            bp_match=re.match(r'(\d+)/(\d+)', sbp_dbp_24h)
            print (bp_match)

            heart_rate_pattern = r'Pulse weighted average\s+(\d+)'
            heart_rate_match=re.search(heart_rate_pattern, first_page_text)
            print(heart_rate_match)
            # extract awake and asleep BPs as recorded by device
            #awake

            sbp_dbp_24h_pattern=re.search(r'(\d+)/(\d+)',sbp_dbp_24h)
            sbp_dbp_awake=avg_bp_match.group(3)
            sbp_dbp_awake_pattern=re.search(r'(\d+)/(\d+)',sbp_dbp_awake)
            sbp_dbp_asleep=avg_bp_match.group(4)
            sbp_dbp_asleep_pattern=re.search(r'(\d+)/(\d+)',sbp_dbp_asleep)

            start_date_match = re.search(r'(\d+/\d+/\d{4})', start_date)
            print (start_date_match)

            print (start_date_match.group(1))
            end_date_match = re.search(r'(\d+/\d+/\d{4})', end_date)
            print (end_date_match)

            # If both start and end date are found, extract and format them
            if start_date_match and end_date_match:
                start_date = datetime.strptime(start_date_match.group(1), '%d/%m/%Y').strftime('%d-%m-%Y')
                print (start_date)
                end_date = datetime.strptime(end_date_match.group(1), '%d/%m/%Y').strftime('%d-%m-%Y')
                print (end_date)
                # Ensure consistent two-digit day format
                #start_date = datetime.strptime(start_date, '%Y-%m-%d').strftime('%d/%m/%Y')
                #end_date = datetime.strptime(end_date, '%Y-%m-%d').strftime('%d/%m/%Y')

                #serial_number = serial_number_match.group(1)
                #print (serial_number)
            else:
                start_date, end_date  = None, None

            # If data on number of successful readings is found, extract and format it
            #if successful_readings_match:
             #  successful_readings = successful_readings_match.group(1)
              # print (successful_readings)

            #else:
            #    successful_readings = None


            if successful_percentage_match:
               success_percent = float(successful_percentage_match.group(1))
               print (success_percent)
               if success_percent < 69.5:
                    poor_quality_reading_list.append(pdf_file)

            else:
                success_percent = None

            # if 24hrs, awake and asleep average sbp and dbp as computed by device is found, extract and format it

            if sbp_dbp_24h_pattern:
               sbp_24h = sbp_dbp_24h_pattern.group(1)
               print(sbp_24h)

               dbp_24h= sbp_dbp_24h_pattern.group(2)
               print(dbp_24h)

            else:
                sbp_24h,dbp_24h = None, None

            if sbp_dbp_awake_pattern:
               sbp_awake = sbp_dbp_awake_pattern.group(1)
               print(sbp_awake)

               dbp_awake= sbp_dbp_awake_pattern.group(2)
               print(dbp_awake)
            else:
                sbp_awake, dbp_awake = None, None


            if sbp_dbp_asleep_pattern:
               sbp_asleep = sbp_dbp_asleep_pattern.group(1)
               print (sbp_asleep)
               dbp_asleep= sbp_dbp_asleep_pattern.group(2)
               print (dbp_asleep)

            else:
                    sbp_asleep,dbp_asleep = None, None


            if heart_rate_match:
               hr_24h=heart_rate_match.group(1)
               print (hr_24h)
            else:
                hr_24h=None

            # Use tabula to extract tables from the PDF-relevant tables start on P2 of the pdf files
            # 2 tables extracted separately from p2 and p3 using lattice method and then concatenated
            # 3 separate extraction needed because T column in raw table is causing problems

            table_p2_times = read_pdf(pdf_path, pages=[2], format="csv",
            lattice = True,area=[(85, 70, 1200, 100)], )
            print (table_p2_times)

            table_p2_bps = read_pdf(pdf_path, pages=[2], format="csv",
            lattice = True,area=[(95, 120, 1200, 220)], )
            print (table_p2_bps)

            array_times = np.array(table_p2_times)
            array_bps = np.array(table_p2_bps)
            print (array_bps)

            # Reshape the array to make it two-dimensional
            reshaped_times = array_times.reshape(-1, array_times.shape[-1])
            reshaped_bps = array_bps.reshape(-1, array_bps.shape[-1])

            # Convert the reshaped array to a DataFrame

            table_p2_times_df = pd.DataFrame(reshaped_times)
            print (table_p2_times_df)


            table_p2_bps_df = pd.DataFrame(reshaped_bps)
            print (table_p2_bps_df)

            table_p2 = pd.concat([table_p2_times_df, table_p2_bps_df], axis=1)
            print (table_p2)
            # Remove all colons in the entire DataFrame
            table_p2 = table_p2.replace(':', '', regex=True)
            print (table_p2)
            # filter to remove measurements that were taken before start Time
            table_p2 = table_p2.apply(pd.to_numeric, errors='coerce')
            print (table_p2)

            column_names_p2= ['Time','sbp','dbp','HR']
            table_p2.columns=column_names_p2
            print (table_p2)

            table_p2 = table_p2[(table_p2['Time'] >= start_time) | (table_p2['Time'] < 800)]
            #table_p2= table_p2[table_p2['Time'] >= start_time] this was leading to some rows being mistakenly dropped
            print (table_p2)

            table_p2['Day'] = table_p2['Time'].apply(lambda x: 1 if 800 <= x < 2359 else 0)

            # Determine the largest time where Time > 2300
            time_greater_2300 = table_p2[table_p2['Time'] > 2300]['Time']
            if not time_greater_2300.empty:
                max_time_after_2300 = time_greater_2300.max()
                print (max_time_after_2300)
                index_max_time_after_2300 = table_p2[table_p2['Time'] == max_time_after_2300].index[0]
                print (index_max_time_after_2300)
                print (len(table_p2))

                # Update the 'Day' value to 2 for the row immediately after the row where Time is max_time_after_2300
                if index_max_time_after_2300 + 1 < len(table_p2):
                    table_p2.loc[index_max_time_after_2300 + 1:, 'Day'] = 2

            print (table_p2)

            # Reordering columns so that 'Day' comes before 'Time'
            table_p2 = table_p2[['Day', 'Time'] + [col for col in table_p2.columns if col not in ['Day', 'Time']]]

            print (table_p2)


            table_p2.reset_index(drop=True, inplace=True)
            print (table_p2)

            # Use tabula to extract tables from the PDF-relevant tables start on P3 of the pdf files

            table_p3_times = read_pdf(pdf_path, pages=[3], format="csv",
            lattice = True,area=[(100, 70, 1200, 100)], ) #, multiple_tables=True)
            print (table_p3_times)

            table_p3_bps = read_pdf(pdf_path, pages=[3], format="csv",
            lattice = True,area=[(107, 120, 1200, 220)], ) #, multiple_tables=True)
            print (table_p3_bps)

            array_times_p3 = np.array(table_p3_times)
            array_bps_p3 = np.array(table_p3_bps)
            print (array_bps_p3)

            # Reshape the array to make it two-dimensional
            reshaped_times_p3 = array_times_p3.reshape(-1, array_times_p3.shape[-1])
            reshaped_bps_p3 = array_bps_p3.reshape(-1, array_bps_p3.shape[-1])

            # Convert the reshaped array to a DataFrame
            table_p3_times_df = pd.DataFrame(reshaped_times_p3)
            print (table_p3_times_df)

            table_p3_bps_df = pd.DataFrame(reshaped_bps_p3)
            print (table_p3_bps_df)


            table_p3 = pd.concat([table_p3_times_df, table_p3_bps_df], axis=1)
            print (table_p3)



            # Remove all colons in the entire DataFrame
            table_p3 = table_p3.replace(':', '', regex=True)
            print (table_p3)
            # filter to remove measurements that were taken before start Time
            table_p3 = table_p3.apply(pd.to_numeric, errors='coerce')
            print (table_p3)

            column_names_p3= ['Time','sbp','dbp','HR']
            table_p3.columns=column_names_p3
            print (table_p3)

            print (end_time)
            table_p3= table_p3[table_p3['Time'] <= end_time]
            print (table_p3)

            # Check if there are any values in 'Day' column of table_p2 that are equal to 2
            if (table_p2['Day'] == 2).any():
                # Create a new 'Day' column in table_p3 with all values set to 2
                table_p3['Day'] = 2
                # Display the resulting table_p3
                print(table_p3)

            # Reordering columns so that 'Day' comes before 'Time'
            table_p3 = table_p3[['Day', 'Time'] + [col for col in table_p3.columns if col not in ['Day', 'Time']]]

            print (table_p3)


            table_p3.reset_index(drop=True, inplace=True)

            print (table_p3)

            table_4= pd.concat([table_p2,table_p3],axis=0)
            print (table_4)
            table_4 = table_4.dropna(how='all')
            print (table_4)

            combined_tables= table_4

            print(combined_tables)

            success_count = combined_tables.notnull().all(axis=1).sum()

            print (success_count)

            total_count = combined_tables.notnull().any(axis=1).sum()

            print (total_count)

            column_names = ['Day','Time','Sys','Dia','HR']
            combined_tables.columns = column_names

            print (combined_tables)

            combined_tables.reset_index(drop=True, inplace=True)

            serial_number = 0  # Omron 24/7 indentifier
            #print (study_id)

            #test="test.csv"
            #output_csv_path_test = os.path.join("/Users/aetyang/Documents/Work folders/IHCOR Africa/Data/24hABPM/output/",test)

            #combined_tables.to_csv(output_csv_path_test,sep=',', index=False)

            # Check if any tables were extracted
            if  combined_tables.empty:
                print(f"No tables found in {pdf_file}. Skipping...")
                continue

            # Extract columns 2-5 and 8 only
            print(combined_tables.shape[1]) # no of columns
            print(combined_tables.shape[0]) # no of rows
            print (combined_tables.iloc[:, list(range(0, 4))].copy())
            if combined_tables.shape[1] >= 5:

                # Create a DataFrame for metadata
                metadata = pd.DataFrame({
                    'study_id': [study_id] * combined_tables.shape[0],
                    'StartDate': [start_date] * combined_tables.shape[0],
                    'EndDate': [end_date] * combined_tables.shape[0],
                    'SerialNumber': [serial_number] * combined_tables.shape[0],
                    'successful_readings': [success_count] * combined_tables.shape[0],
                    'total_readings': [combined_tables.shape[0]] * combined_tables.shape[0],
                    'success_percent': [success_percent] * combined_tables.shape[0],
                    'sbp_24h': [sbp_24h] * combined_tables.shape[0],
                    'dbp_24h': [dbp_24h] * combined_tables.shape[0],
                    'hr_24h': [hr_24h] * combined_tables.shape[0],
                    'sbp_awake': [sbp_awake] * combined_tables.shape[0],
                    'dbp_awake': [dbp_awake] * combined_tables.shape[0],
                    'sbp_asleep': [sbp_asleep] * combined_tables.shape[0],
                    'dbp_asleep': [dbp_asleep] * combined_tables.shape[0],

                })
                # Format the dates in the metadata DataFrame
                #metadata['StartDate'] = pd.to_datetime(metadata['StartDate'], format='%d/%m/%Y').dt.strftime('%d-%m-%Y')
                #metadata['EndDate'] = pd.to_datetime(metadata['EndDate'], format='%d/%m/%Y').dt.strftime('%d-%m-%Y')


                print(metadata)
                print(metadata.shape[0])
                print(metadata.shape[1])

                metadata.reset_index(drop=True, inplace=True)

                # Concatenate metadata and selected_columns
                result_df = pd.concat([metadata, combined_tables], axis=1,ignore_index=True)
                print (result_df)

                print (result_df.columns)

                result_df_column_names = ['study_id',
                                          'StartDate',
                                          'EndDate',
                                          'SerialNumber',
                                          'successful_readings',
                                          'total_readings',
                                          'success_percent',
                                          'sbp_24h',
                                          'dbp_24h',
                                          'hr_24h',
                                          'sbp_awake',
                                          'dbp_awake',
                                          'sbp_asleep',
                                          'dbp_asleep',
                                          'Day',
                                          'Time',
                                          'Sys',
                                          'Dia',
                                          'HR',
                                                    ]

                result_df.columns = result_df_column_names

                print (result_df)

                # Append the result_df to the overall combined_data DataFrame
                combined_data = pd.concat([combined_data, result_df], ignore_index=True)
                print(combined_data)
                combined_data.columns = result_df_column_names
                print (combined_data)
            else:
                print(f"Skipping {pdf_file} due to insufficient columns in the tables.")


        except Exception as e:
            print(f"Error processing {pdf_file}: {e}")
            continue  # Skip to the next iteration of the loop
        #finally:
            # Close the PDF document
            pdf_document.close()

# Export the combined_data DataFrame to a CSV file
if not combined_data.empty:
    combined_data.to_csv(output_csv_path, index=False)
    print("Extraction and export completed.")
else:
    print("No tables found in any PDF files.")

# Export the list of PDFs with less than 70% successful readings to a CSV file
if poor_quality_reading_list:
    file_name=f"poor_quality_reading_list_omron24_7_{today_date}.csv"
    print(file_name)
    poor_quality_reading_list_df = pd.DataFrame({'poor quality abpm readings': poor_quality_reading_list})
    poor_quality_reading_list_df.to_csv(os.path.join("/Users/aetyang/Documents/Work folders/IHCOR Africa/Data/24hABPM/output/",file_name), index=False)
    print("PDFs with less than 70% successful readings exported.")

# Export the list of failed PDFs to a CSV file
if raw_tables_missing_list:
    file_name=f"raw_tables_missing_list_gambia_{today_date}.csv"
    raw_tables_missing_list_df = pd.DataFrame({'Raw Tables Missing': raw_tables_missing_list})
    raw_tables_missing_list_df.to_csv(os.path.join("/Users/aetyang/Documents/Work folders/IHCOR Africa/Data/24hABPM/output/",file_name), index=False)
    print("Failed PDFs list exported.")
else:
    print("No PDFs failed during extraction.")

