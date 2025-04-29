![FBB_Logo](https://firmbackbone.nl/wp-content/uploads/sites/694/2025/03/FBB-logo-wide.png)

**[FIRMBACKBONE](https://firmbackbone.nl)** is an organically growing longitudinal data-infrastructure with information on Dutch companies for scientific research and education. Once it is ready, it will become available for researchers and students affiliated with Dutch member universities through the Open Data Infrastructure for Social Science and Economic Innovations ([ODISSEI](https://odissei-data.nl/nl/)). FIRMBACKBONE is an initiative of Utrecht University ([UU](https://www.uu.nl/en)) and the Vrije Universiteit Amsterdam ([VU Amsterdam](https://vu.nl/en)) funded by the Platform Digital Infrastructure-Social Sciences and Humanities ([PDI-SSH](https://pdi-ssh.nl/en/front-page/)) for the period 2020-2025.

## KVK Import Script

This repository consists of several PySpark scripts that import KVK data into the backbone (or createes a new one), generates gold datasets for selected subsets and generates DDI 2.5 compliant metadata in XML format as well 
as in PDF. For the scripts to work, a version of Apache Spark must be accessible (currently only a locally hosted Spark is used). Some additional packages are required:

- colorama==0.4.6 (used for the color scheme on the terminal window's logging)
- findspark==2.0.1 (used for configuring the locally hosted Spark sessions)
- fpdf==1.7.2 (used for generating the metadata into a human readable PDF file)
- lxml==5.3.0 (used for generating the metadata into a DDI 2.5 compliant XML file)
- pyspark==3.5.1 (used for most data actions, executed on an Apache Spark instance)
- pyspark==3.5.3 (used for most data actions, executed on an Apache Spark instance)
- numpy==1.26.4 (in case of SPSS file, Spark is not compatible with numpy>2)

To work with the scripts, please read the documentation within the code carefully. The scripts are numbered in terms of execution order. The first script copies the data into the bronze folder and updates the backbone with the changes, after it links the source data with the backbone identifiers and saves a full copy in the silver data. The second script, subsets the data and filters for specific elements prior to storing the speudonimized data into the gold folder. Finally the third script generates the XML and PDF metadata in DDI 2.5 compatible format.

## Usage

Typically the numbered scripts are executed in sequence:

- Run `01_import_kvk.py` to load the KVK data from the provided CSV file (bronze data) and link it to an existing backbone or create a new backbone (silver data).
- Run `02_generate_gold.py` to pseudonimize the silver data and store in the final data format (gold data).
- Run `03_generate_ddi_v2.py` to generate a DDI 2.5 compliant XML file and a similar PDF file for documentation purposes (specify the `ddi_kvk_template.json` file as variable template).
- Run `04_link_lisa.py` to load the LISA data from the provided Stata file (bronze data) and link it to an excisting backbone (silver data).
- Run `05_gold_lisa.py` to pseudonimize the silver data and store in the final data format (gold data).
- Run `03_generate_ddi_v2.py` to generate a DDI 2.5 compliant XML file and a similar PDF file for documentation purposes (but refer to the LISA data instead and specify the `ddi_lisa_template.json`).

The file `firmbackbone.py` and `lisa.py` contain frequently used and domain specific functions and transformations that are required during the process.

## More information

Please visit the [FIRMBACKBONE](https://firmbackbone.nl) website for more information on the project, contact options or email [Peter Gerbrands](https://pgerbrands.github.io/) with specific questions about these scripts.

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](https://choosealicense.com/licenses/mit/)
