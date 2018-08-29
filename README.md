# SQL_ETL-using-flush-and-fill
This repository contains SQL scripts that create both OLTP and OLAP database and synchronize them using 'Flush and Fill' technique.

- 01_StarterScript Create the OLTP DB file is SQL script that creates OLTP database.
- 02_SetupScript Create the DW DB file is SQL script which creates OLAP database.
- ETL_flush-and-fill file is SQL script which synchronize OLTP and OLAP database using flush and fill technique. After synchronization, OLAP database will reflect exact same data from OLTP database.

Once the procedures are successfully executed, OLAP database will be used for reporting purposes.
