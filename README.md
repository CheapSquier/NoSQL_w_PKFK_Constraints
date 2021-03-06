# NoSQL_w_PKFK_Constraints
Comparison of Aerospike key-value NoSQL database with PK and FK constraints added, vs SQL

The goal of this project is to add PK and FK constraint checking functionality normally found in relational databases to a 'NoSQL' key-value database. The key-value DB used is the Aerospike database but the concept should be similar for other key-value implementations.

With that functionality added, we implement the same schemas on a MySQL database and benchmark execution times for common database
operations such as table creation, data insertion, updates, and deletion. Data, including mean, median, and std dev, is stored in a csv file for each test case of each database. A comparison of execution times between Aerospike (key-value) and MySQL (relational) can be performed using the Jupyter Notebook AS_vs_SQL_analysis.ipynb.
