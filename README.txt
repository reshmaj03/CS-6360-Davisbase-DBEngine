CS-6360 Database Design
Programming Project: Relational Database Internals
----------------------------------------------------

NOTE: The Project requires Java 8 version.

Steps to start Davisbase:
*************************
1.	Go to directory src.
2.	Compile Prompt.java using the command below:
		javac Prompt.java 
3.	Execute the generated Prompt class file using below command to launch the database engine.
		java Prompt
4.	Once launched, the queries can be executed.




SUPPORTED COMMANDS
**********************

All commands below are case insensitive

SHOW TABLES;
	Display the names of all tables.

CREATE TABLE <table_name>;
	Creates table as per the schema specified.

SELECT <column_list> FROM <table_name> [WHERE <condition>];
	Display table records whose optional <condition>
	is <column_name> = <value>.

DROP TABLE <table_name>;
	Remove table data (i.e. all records) and its schema.

UPDATE TABLE <table_name> SET <column_name> = <value> [WHERE <condition>];
	Modify records data whose optional <condition>
	is <column_name> = <value>.

INSERT INTO <table_name> <column_list> values(<value_list>);
	Insert data into table.

DELETE FROM <table_name> [WHERE <condition>];
	Deletes record data whose optional <condition> is
	is <column_name> = <value>.

VERSION;
	Display the program version.

HELP;
	Display this help information.

EXIT;
	Exit the program.

