## About

This repository achieves **Temporal Data Management** in Exasol with the help of **query rewriting** using a Preprocessor script. Temporal data refers to data that changes over time, and this feature enables tracking historical records while allowing for time-based queries. More indepth information about this proof of concept implementation can be found in the scientific paper *A Query-Rewriting-based Implementation for Temporal Data Management*.

## Usage 

Download the *Preprocessor.sql* file and execute all the commands it contains in your exasol instance. To activate the preprocessor use the following command: *ALTER SESSION SET sql_preprocessor_script = mypreprocessor;*

After activation, each newly created table has historical storage. This is accomplished using a history table and a SQL view with only current data. Both tables can be queried like expected.

INSERT, UPDATE, DELETE

**Note:** Due to its prototype nature this implementation does not support all possible SQL commands. Each successful query is confirmed with a status message e.g. how many rows are affected by this change.

One of the main feature of this tool is to retrieve outdated data. For this, a *SELECT* query needs to be extended by a *AS OF SYSTEM TIME* clause. This looks like *SELECT <column_name> FROM <table_name> AS OF SYSTEM TIME 'YYYY-MM-DD HH:mm';*

If historical data storage is no longer required, the preprocessor can be deactivated with the following command *ALTER SESSION SET sql_preprocessor_script = mypreprocessor;*
