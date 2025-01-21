import json
import os
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from lxml import etree
from fpdf import FPDF

import findspark, uuid, os, shutil
from pyspark.sql import SparkSession
from datetime import date
from colorama import Fore, Back
import datetime

os.system('cls')

(t_width, t_height) = os.get_terminal_size()

COLOUR_HEAD = Fore.WHITE + Back.RED
COLOUR_TASK = Fore.BLUE + Back.WHITE
COLOUR_STEP = Fore.WHITE + Back.LIGHTBLUE_EX
COLOUR_CONF = Fore.GREEN + Back.LIGHTYELLOW_EX
COLOUR_REST = Fore.RESET + Back.RESET

print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
print(COLOUR_HEAD + '  Utrecht University     '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '  FIRMBACKBONE v3.00     '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '  20250114: DDI Exporter '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + f'  Start: {datetime.datetime.now()}'.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)

########################################################################
### Configure environment
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Loading configuration...'.ljust(t_width) + COLOUR_REST)

# Paths to input files
file_location = 'C:/Users/5775620/Documents/FirmBackbone/pySpark_ETL/gold/'
source_file = '20231221_kvk_legal_profit'
codebook_xml_file = f'ddi_{source_file}.xml'
codebook_pdf_file = f'ddi_{source_file}.pdf'
metadata_json = f'ddi_{source_file}.json'

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Initialize Spark...'.ljust(t_width) + COLOUR_REST)
findspark.init('c:/spark')
spark = SparkSession.builder \
    .appName('fbb_ddi_exporter') \
    .config('spark.driver.memory', '60g') \
    .config('spark.sql.parquet.datetimeRebaseModeInWrite', 'CORRECTED') \
    .config('spark.worker.cleanup.enabled', 'true') \
    .getOrCreate()    

########################################################################
### Initialize data
########################################################################

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Loading data from Parquet...'.ljust(t_width) + COLOUR_REST)
df = spark.read.parquet(file_location + source_file)

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Loading manual metadata from JSON...'.ljust(t_width) + COLOUR_REST)
with open(file_location + metadata_json, "r") as f:
    metadata = json.load(f)

########################################################################
### Helper functions
########################################################################

def get_file_size(file_path):
    print(f'    {datetime.datetime.now()} :: Retrieving file size...')
    return os.path.getsize(file_path)

def compute_statistics(column_name, column_data_type):
    print(f'    {datetime.datetime.now()} :: Calculating statistics for column {column_name}...')
    stats = {}
    if column_data_type in ["int", "bigint", "double", "float"]:
        stats["min"] = df.select(F.min(column_name)).collect()[0][0]
        stats["max"] = df.select(F.max(column_name)).collect()[0][0]
        stats["mean"] = df.select(F.mean(column_name)).collect()[0][0]
        stats["stddev"] = df.select(F.stddev(column_name)).collect()[0][0]
    elif column_data_type in ["string"]:
        stats["distinct_count"] = df.select(column_name).distinct().count()
        stats["null_count"] = df.filter(F.col(column_name).isNull()).count()
    return stats

########################################################################
### Codebook Generation Process
########################################################################

# Generate the DDI XML codebook
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Generate DDI codebook...'.ljust(t_width) + COLOUR_REST)
ddi = etree.Element("DDIInstance", nsmap={"ddi": "http://www.ddialliance.org/DDI"})
document_description = etree.SubElement(ddi, "docDscr")
data_description = etree.SubElement(ddi, "dataDscr")

# Add study-level metadata
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Adding external study information...'.ljust(t_width) + COLOUR_REST)
if "study" in metadata:
    study = metadata["study"]
    citation = etree.SubElement(document_description, "citation")
    title_stmt = etree.SubElement(citation, "titlStmt")
    etree.SubElement(title_stmt, "titl").text = study.get("title", "Unknown Title")
    etree.SubElement(title_stmt, "IDNo").text = study.get("id", "Unknown ID")
    prod_stmt = etree.SubElement(citation, "prodStmt")
    etree.SubElement(prod_stmt, "producer").text = study.get("producer", "Unknown Producer")
    etree.SubElement(document_description, "abstract").text = study.get("abstract", "No abstract provided")
    etree.SubElement(document_description, "dataAccs", type="license").text = study.get("license_terms", "No license terms provided")

# Add file-level metadata
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Adding file level metadata...'.ljust(t_width) + COLOUR_REST)
file_description = etree.SubElement(document_description, "fileDscr")
file_name = os.path.basename(file_location + source_file)
file_size = get_file_size(file_location + source_file)
row_count = df.count()
column_count = len(df.columns)

etree.SubElement(file_description, "fileName").text = file_name
etree.SubElement(file_description, "fileSize").text = str(file_size)
file_dims = etree.SubElement(file_description, "dimensns")
etree.SubElement(file_dims, "caseQnty").text = str(row_count)
etree.SubElement(file_dims, "varQnty").text = str(column_count)

# Add column-level metadata
print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Adding column level metadata...'.ljust(t_width) + COLOUR_REST)
for column in df.schema:
    column_name = column.name
    column_data_type = column.dataType.simpleString()
    # Create variable element for each column
    variable = etree.SubElement(data_description, "var", name=column_name)
    # Add description from metadata JSON if available
    column_metadata = metadata.get("columns", {}).get(column_name, {})
    etree.SubElement(variable, "labl").text = column_metadata.get("description", "No description provided")
    # Add data type
    etree.SubElement(variable, "varFormat", type=column_data_type)
    # Add value labels if available
    if "values" in column_metadata:
        print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Adding category documentation...'.ljust(t_width) + COLOUR_REST)
        for value, label in column_metadata["values"].items():
            value_element = etree.SubElement(variable, "catgry")
            etree.SubElement(value_element, "catValu").text = str(value)
            etree.SubElement(value_element, "labl").text = label
    # Compute and add statistics
    stats = compute_statistics(column_name, column_data_type)
    for stat_name, stat_value in stats.items():
        etree.SubElement(variable, "sumStat", type=stat_name).text = str(stat_value)

# Write the XML tree to file
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Write XML file to disk...'.ljust(t_width) + COLOUR_REST)
xml_tree = etree.ElementTree(ddi)
with open(file_location + codebook_xml_file, "wb") as xml_file:
    xml_tree.write(xml_file, pretty_print=True, xml_declaration=True, encoding="UTF-8")

print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: DDI Codebook completed and saved as {file_location + codebook_xml_file}...'.ljust(t_width) + COLOUR_REST)

########################################################################
### Generate PDF file
########################################################################

# Generate a human-readable PDF report
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Generate metadata PDF file...'.ljust(t_width) + COLOUR_REST)
def generate_pdf(metadata, file_name, file_size, row_count, column_count):
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    pdf.set_font("Times", size=12)

    # Add title
    pdf.set_font("Times", style="B", size=16)
    pdf.cell(0, 10, "Dataset Metadata Report", ln=True, align="C")
    pdf.ln(10)

    # Add study-level metadata
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Adding study level metadata...'.ljust(t_width) + COLOUR_REST)
    if "study" in metadata:
        pdf.set_font("Times", style="B", size=14)
        pdf.cell(0, 10, "Study Information", ln=True)
        pdf.set_font("Times", size=12)
        study = metadata["study"]
        pdf.cell(0, 10, f"Title: {study.get('title', 'Unknown Title')}", ln=True)
        pdf.cell(0, 10, f"ID: {study.get('id', 'Unknown ID')}", ln=True)
        pdf.cell(0, 10, f"Producer: {study.get('producer', 'Unknown Producer')}", ln=True)
        pdf.multi_cell(0, 10, f"Abstract: {study.get('abstract', 'No abstract provided')}")
        pdf.cell(0, 10, f"License Terms: {study.get('license_terms', 'No license terms provided')}", ln=True)
        pdf.ln(10)

    # Add file-level metadata
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Adding file level metadata...'.ljust(t_width) + COLOUR_REST)
    pdf.set_font("Times", style="B", size=14)
    pdf.cell(0, 10, "File Information", ln=True)
    pdf.set_font("Times", size=12)
    pdf.cell(0, 10, f"Filename: {file_name}", ln=True)
    pdf.cell(0, 10, f"File Size: {file_size} bytes", ln=True)
    pdf.cell(0, 10, f"Row Count: {row_count}", ln=True)
    pdf.cell(0, 10, f"Column Count: {column_count}", ln=True)
    pdf.ln(10)

    # Add column-level metadata
    print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Adding column level metadata...'.ljust(t_width) + COLOUR_REST)
    pdf.set_font("Times", style="B", size=14)
    pdf.cell(0, 10, "Column Information", ln=True)
    pdf.set_font("Times", size=12)
    for column in df.schema:
        column_name = column.name
        column_data_type = column.dataType.simpleString()
        column_metadata = metadata.get("columns", {}).get(column_name, {})
        description = column_metadata.get("description", "No description provided")        
        stats = compute_statistics(column_name, column_data_type)

        pdf.set_font("Times", style="B", size=12)
        pdf.cell(0, 10, f"Column: {column_name}", ln=True)
        pdf.set_font("Times", size=12)
        pdf.cell(0, 10, f"Type: {column_data_type}", ln=True)
        pdf.multi_cell(0, 10, f"Description: {description}")
        
        # Value Labels (if any)
        if "values" in column_metadata:
            print(COLOUR_STEP + f'    {datetime.datetime.now()} :: Adding column level metadata...'.ljust(t_width) + COLOUR_REST)
            pdf.set_font("Times", style="B", size=12)
            pdf.cell(40, 10, "Values: ", ln=True)
            pdf.set_font("Times", size=12)
            for value, label in column_metadata["values"].items():
                pdf.cell(20, 10, f"  {value}: ", ln=0)
                pdf.cell(0, 10, label, ln=True)

        # Add statistics in a table-like structure
        pdf.cell(50, 10, "Statistic", border=1, ln=0, align="C")
        pdf.cell(50, 10, "Value", border=1, ln=1, align="C")
        for stat_name, stat_value in stats.items():
            pdf.cell(50, 10, stat_name, border=1, ln=0, align="C")
            pdf.cell(50, 10, str(stat_value), border=1, ln=1, align="C")
        pdf.ln(5)

    # Save the PDF
    pdf.output(file_location + codebook_pdf_file)
    print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: PDF Codebook completed and saved as {file_location + codebook_pdf_file}...'.ljust(t_width) + COLOUR_REST)

# Generate the PDF report
print(COLOUR_TASK + f'>>> {datetime.datetime.now()} :: Generating PDF Codebook...'.ljust(t_width) + COLOUR_REST)
generate_pdf(metadata, file_name, file_size, row_count, column_count)

########################################################################
### Stop the Spark session
########################################################################

spark.catalog.clearCache()
spark.stop()

print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
print(COLOUR_HEAD + '  20250114: DDI Codebook completed! '.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + f'  Finish: {datetime.datetime.now()}'.ljust(t_width) + COLOUR_REST)
print(COLOUR_HEAD + '-' * t_width + COLOUR_REST)
