
# this script does the following:
# 1. extracts metadata from spacelabs 24hr reports (these are in pdf format)
#2. extracts relevant columns from the raw data presented in tables within the pdf_file
# 3. assembles all of the raw data and metadata into one csv file in long format
# 4. Author: Anthony O. Etyang. Last update 21 Nov 2023

# import libraries that will be needed for the task
import os
import pandas as pd
from tabula import read_pdf # for reading tables in pdfs
import fitz  # PyMuPDF for reading text in pdfs
import re # regular expressions
from datetime import datetime

# Set the path to the folder containing the spacelabs PDF files
pdf_folder_path = "/Users/aetyang/Documents/Work folders/IHCOR Africa/Data/24hABPM/spacelabs"

# Set the path for the output CSV file
output_csv_path = "/Users/aetyang/Documents/Work folders/IHCOR Africa/Data/24hABPM/output/spacelabs_results.csv"

# Create an empty DataFrame to store the extracted data
combined_data = pd.DataFrame()

# loop through each PDF file in the folder
for pdf_file in os.listdir(pdf_folder_path):
    if pdf_file.endswith(".pdf"):
        # Extract identifier from file name
        study_id = pdf_file.split('_')[0]
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
             # 24hr avg heart rate (still struggling to extract this as at 21 Nov 2023)

            start_date_match = re.search(r'Start:\s*(\d{2}/\d{2}/\d{4})', first_page_text)
            end_date_match = re.search(r'End:\s*(\d{2}/\d{2}/\d{4})', first_page_text)
            serial_number_match = re.search(r'Serial number:\s*(\d{5}-\d{6})', first_page_text)
            successful_readings_match = re.search(r'Successful:\s*([\d.]+)% \((\d+) of (\d+)\)', first_page_text)
            avg_bp_match=re.search(r'Avg.:\s*(\d+/\d+) mmHg', first_page_text)
            sbp_dbp_24h=avg_bp_match.group(1)
            bp_match=re.match(r'(\d+)/(\d+)', sbp_dbp_24h)
            #heart_rate_match=re.search(r'Heart rate \(BPM)\s*(\d+)', first_page_text)

            # If both start and end date are found, extract and format them
            if start_date_match and end_date_match:
                start_date = datetime.strptime(start_date_match.group(1), '%d/%m/%Y').strftime('%d-%m-%Y')
                end_date = datetime.strptime(end_date_match.group(1), '%d/%m/%Y').strftime('%d-%m-%Y')
                serial_number = serial_number_match.group(1)
            else:
                start_date, end_date, serial_number = None, None, None

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

            # Print extracted values for debugging
            print("Start Date:", start_date_match.group(1) if start_date_match else None)
            print("End Date:", end_date_match.group(1) if end_date_match else None)
            print("Serial Number:", serial_number_match.group(1) if serial_number_match else None)
            print(successful_readings_match)
            print(avg_bp_match)
            print(sbp_dbp_24h)
            print(bp_match)
            print(sbp_24h)
            print(dbp_24h)
            #print(heart_rate_match)
            print("Success Percentage:", successful_readings_match.group(1) if successful_readings_match else None)
            print("Success Count:", successful_readings_match.group(2) if successful_readings_match else None)
            print("Total Count:", successful_readings_match.group(3) if successful_readings_match else None)


            # Print formatted values for debugging
            print("Formatted Start Date:", start_date)
            print("Formatted End Date:", end_date)
            print("Formatted Serial Number:", serial_number)

            # Use tabula to extract tables from the PDF-relevant tables start on P4 of the pdf files
            tables = read_pdf(pdf_path, pages=list(range(4, n_pages + 1)), multiple_tables=True)
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
                selected_columns = combined_tables.iloc[:, list(range(1, 5)) + [7]].copy()
                #print(selected_columns)

                # Create a DataFrame for metadata
                metadata = pd.DataFrame({
                    'study_id': [study_id] * selected_columns.shape[0],
                    'StartDate': [start_date] * selected_columns.shape[0],
                    'EndDate': [end_date] * selected_columns.shape[0],
                    'SerialNumber': [serial_number] * selected_columns.shape[0],
                    'successful_readings': [success_count] * selected_columns.shape[0],
                    'total_readings': [total_count] * selected_columns.shape[0],
                    'success_percent': [success_percentage] * selected_columns.shape[0],
                    'sbp_24h': [sbp_24h] * selected_columns.shape[0],
                    'dbp_24h': [dbp_24h] * selected_columns.shape[0],
                })

                # Concatenate metadata and selected_columns
                result_df = pd.concat([metadata, selected_columns], axis=1)

                # Append the result_df to the overall combined_data DataFrame
                combined_data = pd.concat([combined_data, result_df], ignore_index=True)
                print(combined_data)
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
