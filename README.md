# NoSQL_w_PKFK_Constraints
Comparison of Aerospike key-value NoSQL database with PK and FK constraints added, vs SQL

The goal of this project is to add PK and FK constraints normally found in relational databases to a 'NoSQL' key-value 
database. The particular key-value DB is the Aerospike database but the concept should be similar for other key-value implementations.

With the funcationality added, we implement the same schemas on a MySQL database and benchmark execution times for common database
operations such as table creation, data insertion, updates, and deletion.
