## About

This repository achieves **Temporal Data Management** in Exasol with the help of **query rewriting** using a Preprocessor script. Temporal data refers to data that changes over time, and this feature enables tracking historical records while allowing for time-based queries. More indepth information about this proof of concept implementation can be found in the scientific paper *A Query-Rewriting-based Implementation for Temporal Data Management*.

## Usage 

Download the *Preprocessor.sql* file and execute all the commands it contains in the right order in your exasol instance. To activate the preprocessor use the following command:<br> 
*ALTER SESSION SET sql_preprocessor_script = mypreprocessor;*.

After activation, each newly created table has historical storage. This is accomplished using a history table and a SQL view with only current data. Both tables can be queried like expected. Tables can be created with commands such as:<br>
*CREATE TABLE <table_name> (<column_name> \<DATATYPE\>, ...);* or *CREATE TABLE <new_tbl> LIKE <orig_tbl>;*

The following commands can be used to add new data records:<br>
*INSERT INTO <table_name> (<column_name>, ...) VALUES (<\value\>, ...);*<br>
*INSERT INTO <table_name> VALUES (<\value\>, ...);*<br>
*INSERT INTO <table_name> (<column_name>, ...) SELECT (<column_name>, ...) FROM <other_table>;*

To update an existing data set the subsequent command can be used:<br>
*UPDATE <table_name> SET <column_name> = \<value\>, ... WHERE \<condition\>;* 

Data can be invalidated with a *DELETE* command. This syntax can also be used to empty a whole table without a *WHERE* clause. <br>
*DELETE FROM <tbl_name> WHERE \<condition\>;*

**Note: Due to its prototype nature this implementation does not support all possible SQL commands. Each successful query is confirmed with a status message e.g. how many rows are affected by this change.**

One of the main feature of this tool is to retrieve outdated data. For this, a *SELECT* query needs to be extended by a *AS OF SYSTEM TIME* clause. This should look like *SELECT <column_name> FROM <table_name> AS OF SYSTEM TIME 'YYYY-MM-DD HH:mm';*

If historical data storage is no longer required, the preprocessor can be deactivated with the following command:<br> 
*ALTER SESSION SET sql_preprocessor_script = null;*
