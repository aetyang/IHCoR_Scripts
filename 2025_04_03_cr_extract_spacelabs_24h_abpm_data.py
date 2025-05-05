
# this script does the following:
# 1. extracts metadata from spacelabs 24hr reports (these are in pdf format)
#2. extracts relevant columns from the raw data presented in tables within the pdf_file
# 3. assembles all of the raw data and metadata into one csv file in long format
# 4. Author: Anthony O. Etyang. Last update 21 Nov 2023
#5- updates 25 Nov 2023:
    # study_id is extracted as the first 7 characters of the pdf file name (e.g. KEN0001)
    # additional metadata extracted as recorded/computed by the device-24hr avg heart rate, awake and asleep BPs
    # still unable to deal with 'Day' column where some values show up as -1, -2 instead of 1,2- but this is a minor issue
    # also dates 01-09 of the month have a different format from 10th and later, but this can be fixed in Stata/R (will attempt to update later)
#6 updates 27 Nov 2023
    # sort the pdf files before extracting the tables, so that output is also sorted by study_id

# updates 11 Feb 2025
    # 24hr heart rate extraction code corrected
    # corrected bugs that were leading to data from 47 pdf files not being extracted
        # modified lines 101-106 extraction of sleep BP data
        #line 199-206 extract tables from p4 of pdf only if there were limited data
        # lines 81,82, 122, 139 date extraction and serial number extraction

# import libraries that will be needed for the task
import os
import pandas as pd
from tabula import read_pdf # for reading tables in pdfs
import fitz  # PyMuPDF for reading text in pdfs
import re # regular expressions
from datetime import datetime
from dateutil import parser # for parsing different date formats

# Set the path to the folder containing the spacelabs PDF files
pdf_folder_path = "/Users/path to where you the pdf files are"

# Set the path for the output CSV file
output_csv_path = "/Users/path to where you want to store the output.csv"

# sort the pdf files by name-allows CSV output to be sorted
pdf_files = sorted([f for f in os.listdir(pdf_folder_path) if f.endswith(".pdf")])

print(pdf_files)
len(pdf_files) # no of pdf files in list (786 at last count 10 Dec 2024)

# Create an empty DataFrame to store the extracted data
combined_data = pd.DataFrame()




# loop through each PDF file in the folder
#for pdf_file in pdf_files:
for pdf_file in pdf_files:
    if pdf_file.endswith(".pdf"): # Extract identifier from file name
       study_id = pdf_file[:7] # study_id is composed of the first seven characters of the file name
       print(study_id)

        # Construct the full path to the PDF file
       pdf_path = os.path.join(pdf_folder_path, pdf_file)

       try:
             # Get the number of pages in the PDF
            pdf_document = fitz.open(pdf_path)
            n_pages = pdf_document.page_count
            print(n_pages)

            # Check if the PDF has fewer than 4 pages. Raw tables usually start from P4 of the pdf
            if n_pages < 4:
                print(f"{study_id} has less than 4 pages-Probable error. Skipping...")
                continue  # Skip to the next iteration of the loop

            # Extract text from the first page (dates and other metadata are on the first page)
            first_page_text = pdf_document[0].get_text()
            print(first_page_text)

            # Extract the following using regular expressions:
             #start and end dates
             # serial number of the device
             # number of successful readings
             # 24hr sbp, 24hr dbp as computed by the spacelabs device (this is to allow comparison with calculations based on raw data)
             # 24hr avg heart rate (still struggling to extract this as at 21 Nov 2023)-update 28 Nov 2023- this has been resolved

            start_date_match = re.search(r'Start:\s*(\d{1,2}/\d{1,2}/\d{4})', first_page_text)
            if start_date_match:
                raw_date = start_date_match.group(1)
                try:
                    start_date = parser.parse(raw_date).strftime('%Y-%m-%d')  # Convert to YYYY-MM-DD format
                except ValueError:
                    print(f"Error parsing date: {raw_date}")
            else:
                start_date = None
            #print (start_date)

            end_date_match = re.search(r'End:\s*(\d{1,2}/\d{1,2}/\d{4})', first_page_text)
            if end_date_match:
                raw_date = end_date_match.group(1)
                try:
                    end_date = parser.parse(raw_date).strftime('%Y-%m-%d')  # Convert to YYYY-MM-DD format
                except ValueError:
                    print(f"Error parsing date: {raw_date}")
            else:
                 end_date = None
            # print(end_date)

            serial_number_match = re.search(r'Serial number:\s*(\d{3,5}-\d{6})', first_page_text)
            successful_readings_match = re.search(r'Successful:\s*([\d.]+)% \((\d+) of (\d+)\)', first_page_text)
            avg_bp_match=re.search(r'Avg.:\s*(\d+/\d+) mmHg', first_page_text)
            sbp_dbp_24h=avg_bp_match.group(1)
            bp_match=re.match(r'(\d+)/(\d+)', sbp_dbp_24h)
            heart_rate_match=re.search(r'Heart rate \(BPM\)\s*(\d+)', first_page_text)
            # extract awake and asleep BPs as recorded by device
            #awake
            wake_periods_index = first_page_text.find("Wake periods summary")
            wake_periods_substring = first_page_text[wake_periods_index:]
            avg_index = wake_periods_substring.find("Avg: ")
            avg_substring = wake_periods_substring[avg_index + len("Avg: "):]
            awake_bp_parts = avg_substring.split()[0]
            awake_match=re.match(r'(\d+)/(\d+)', awake_bp_parts)

            awake_readings_match = re.search(r'Wake periods summary\s*-\s*Successful:\s*\d+.\d+%\s*\((\d+)\s+of\s+\d+\)', first_page_text)
            awake_readings = int(awake_readings_match.group(1)) if awake_match else None
            print(awake_readings)

            sbp_awake=awake_match.group(1)
            dbp_awake=awake_match.group(2)
            #asleep
            Sleep_periods_index = first_page_text.find("Sleep periods summary")
            Sleep_periods_substring = first_page_text[Sleep_periods_index:]
            avg_index = Sleep_periods_substring.find("Avg: ")
            avg_substring = Sleep_periods_substring[avg_index + len("Avg: "):]
            asleep_bp_parts = avg_substring.split()[0]  #if avg_substring and avg_substring.strip() else []
            asleep_match=re.match(r'(\d+)/(\d+)', asleep_bp_parts)
            sbp_asleep=asleep_match.group(1)
            dbp_asleep=asleep_match.group(2)
            print(f"Debug: avg_substring = '{avg_substring}'")


            asleep_readings_match = re.search(r'Sleep periods summary\s*-\s*Successful:\s*\d+.\d+%\s*\((\d+)\s+of\s+\d+\)', first_page_text)
            asleep_readings = int(asleep_readings_match.group(1)) if asleep_readings_match else None
            print(asleep_readings)




            # If serial number is found, extract it
            if serial_number_match:
                serial_number = serial_number_match.group(1)
            else:
                serial_number = None
            print(serial_number_match)
            print (serial_number)

            # If data on number of successful readings is found, extract and format it
            if successful_readings_match:
                success_percentage = float(successful_readings_match.group(1))
                success_count = int(successful_readings_match.group(2))
                total_count = int(successful_readings_match.group(3))

            else:
                success_percentage, success_count, total_count = None, None, None

            # if 24hrs average sbp and dbp as computed by device is found, extract and format it
            if bp_match:
                sbp_24h = bp_match.group(1)
                dbp_24h=bp_match.group(2)

            else:
                sbp_24h,dbp_24h = None, None

            if heart_rate_match:
                hr_24h=heart_rate_match.group(1)
            else:
                hr_24h=None





            # Use tabula to extract tables from the PDF-relevant tables start on P4 of the pdf files
            if n_pages > 4:
                page_range = list(range(4, n_pages + 1))
            else:
                #page_range = list(range(4, n_pages + 0))  # Process  page 4 only if n_pages is 4
                page_range = 4 # Process  page 4 only if n_pages is 4

            tables = read_pdf(pdf_path, pages=page_range, multiple_tables=True)

            #tables = read_pdf(pdf_path, pages=list(range(4, n_pages + 1)), multiple_tables=True)
            print(tables)
            # Check if any tables were extracted
            if not tables:
                print(f"No tables found in {pdf_file}. Skipping...")
                continue

            # Combine the tables into one long table
            combined_tables = pd.concat(tables, ignore_index=True)
            print(combined_tables)
            # Extract columns 2-5 and 8 only
            if combined_tables.shape[1] >= 5:
                selected_columns = combined_tables[['Day', 'Time', 'Sys', 'Dia', 'HR']].copy()
                selected_columns['Day'] = selected_columns['Day'].replace({1: 1, 2: 2})
                print(selected_columns)


                # Create a DataFrame for metadata
                metadata = pd.DataFrame({
                    'study_id': [study_id] * selected_columns.shape[0],
                    'StartDate': [start_date] * selected_columns.shape[0],
                    'EndDate': [end_date] * selected_columns.shape[0],
                    'SerialNumber': [serial_number] * selected_columns.shape[0],
                    'ttl_attempted_readings': [total_count] * selected_columns.shape[0],
                    'ttl_successful_readings': [success_count] * selected_columns.shape[0],
                    'awake_readings': [awake_readings] * selected_columns.shape[0],
                    'asleep_readings': [asleep_readings] * selected_columns.shape[0],
                    'sbp_24h': [sbp_24h] * selected_columns.shape[0],
                    'dbp_24h': [dbp_24h] * selected_columns.shape[0],
                    'hr_24h': [hr_24h] * selected_columns.shape[0],
                    'sbp_awake': [sbp_awake] * selected_columns.shape[0],
                    'dbp_awake': [dbp_awake] * selected_columns.shape[0],
                    'sbp_asleep': [sbp_asleep] * selected_columns.shape[0],
                    'dbp_asleep': [dbp_asleep] * selected_columns.shape[0],

                })
                # Format the dates in the metadata DataFrame
                #metadata['StartDate'] = pd.to_datetime(metadata['StartDate'], format='%d/%m/%Y').dt.strftime('%d-%m-%Y')
                #metadata['EndDate'] = pd.to_datetime(metadata['EndDate'], format='%d/%m/%Y').dt.strftime('%d-%m-%Y')

                # Concatenate metadata and selected_columns
                result_df = pd.concat([metadata, selected_columns], axis=1)

                # Append the result_df to the overall combined_data DataFrame
                combined_data = pd.concat([combined_data, result_df], ignore_index=True)
                if 'PP' in combined_data.columns:
                    combined_data = combined_data.drop(columns=['PP']) # this corrects some erratic behavior
                print(combined_data)
            else:
                print(f"Skipping {pdf_file} due to insufficient columns in the tables.")


       except Exception as e:
            print(f"Error processing {pdf_file}: {e}")
            continue  # Skip to the next iteration of the loop
        #finally:
            # Close the PDF document
            pdf_document.close()



# List of date columns
date_columns = ['StartDate', 'EndDate']

# Convert and format each to consistent dd/mm/yyyy
for col in date_columns:
    combined_data[col] = pd.to_datetime(combined_data[col], errors='coerce').dt.strftime('%d/%m/%Y')
#for col in date_columns:
    #parser.parse(combined_data).strftime('%Y-%m-%d')  # Convert to YYYY-MM-DD format

# Export the combined_data DataFrame to a CSV file
if not combined_data.empty:
    combined_data.to_csv(output_csv_path, index=False)
    print("Extraction and export completed.")
else:
    print("No tables found in any PDF files.")
