# Project 1: Redshift Catalog

This project aims to provide visibility into the system catalog tables generated by Redshift. The use of these tables has played a crucial role in cataloging, monitoring, and governing our data warehouse. As detailed in the article linked to the project, the process of ingesting these tables may pose some challenges, and I hope that the provided code proves helpful.

- **Data Stack**: DBT, Redshift.

- **Medium Article**: [Catalog Tables in Redshift: Enhancing Governance of Your Data Warehouse.](https://medium.com/@alice_thomaz/ee03daf5bcad)

- **Project Organization**
  - :file_folder: DBT - DBT code separated into Governance, Sources, and Macros.
  - :file_folder: Procedures - DDL for table creation and codes for procedures executed on Redshift.