Notice: This document frequently use blinkdb to for the name of commands. This
is because the initial goal was to reproduce BlinkDB on Impala, but the
functions are far from complete. To understand to Full capabilities of
Blinkdb, please refer to BlinkDB website (http://www.blinkdb.org)

This document tells you how to do approximate query using this impala approximate query extension, and about how the code works.

If you want to run approximate query, make sure you read INSTALL.txt and have it installed.

If you just want to find the code, Jump to section "How it works".

1. Procedure for running approximate queries.

	Create sample table from original table.
	Run extension udf on the created Sample table.

1.1 Create sample table

	Let the Table to create sample from be named tableX.
	The following SQL create a 1% sample table from tableX as table sampleTable1. 
	$ CREATE TABLE sampleTableX AS SELECT * FROM tableX WHERE randomSampleWith(0.01);
	You can change the sample size by changing the parameter in function randomSampleWith().

1.2 Query sample table

	After you create the sample table, you can get the approximate count, sum and average of the original table by querying the sample table.

	The available approximate aggregate function includes:
		approx_count: get approximate count. Usage is equivalent to COUNT.
		approx_sum: get approximate sum of one column. Usage is equivalent to SUM.
		approx_avg: get approximate average of one column. Usage is equivalent to AVG.

	For example, you can get the approximate sum of column quantity in table tableX by using approx_sum to query table sampleTableX by issuing the following SQL:
	select approx_sum(quantity) from sampleTableX;

	The result would be a number followed by its confidence region. The result for the preview SQL can be:	3213254 +- 4362 (99% confidence). By saying "99% confidence", it means that 99% of the change the precise value of the sum of quantity in tableX will stay in region [3213254 - 4362, 3213254 + 4362].

	You can add WHERE clause in the end, such as:
	select approx_sum(quantity) from sampleTableX where time > "1993-1-23";

2 How the code works?

2.1 Directory structure
	|- udf: code for user defined functions
		|- randomSampling.cc: define function randomSampleWith
		|- approxCount.cc: define function approx_count
		|- approxAvg.cc: define function approx_avg
		|- approxSum.cc: define function approx_sum
	|- shell: code for a custom shell that does rewriting

2.2 User defined functions

2.2.1 randomSampleWith

	You use function randomSampleWith to create sample table. The signature of randomSampleWith is: 
		Boolean randomSampleWith(Double ratio, int)
		Boolean randomSampleWith(Double ratio, Double)
		Boolean randomSampleWith(Double ratio, String)

	the first argument of randomSampleWith is a double, that is the percentage of sample to create from the original table. The internal of randomSampleWith just generates a random number and compare with this probability. The second parameter of randomSampleWith is an arbitrary column in the original table array. This parameter is there so that the Impala won't see randomSampleWith(0.1) as a constant and only evaluate once for all the rows. The second parameter is added in the custom shell.

2.2.2 approx_sum, approx_count and approx_avg

	The signature of approx_sum are:
		String approx_sum(INT, double sampleWeight) 
		String approx_sum(DOUBLE, double sampleWeight) 

	The first parameter is the column that you would want to sum on. The second parameter is the sampleWeight of the row in the sample table. The sum of the sampleWeight should be an estimate of the size of the query on the original table. We use it together with the size of the approximate query itself, to determine the confidence region.

	Assume sample weight of one sample table is 10(sampleWeight is a constant in a sample table) and there is 1000 rows in the sample table. That means the original table is of size 10 * 1000 = 10000. If 5% of the 1000 rows returns in the approx_sum query with predicates, then the sum of sample weight: 10 * 5% * 1000 should an estimate of the size of the rows we get when we run SUM on the original table with the same predicates. That means you can create sample table with no predicates and run approximate query with the predicated on the sample table.

	approx_count work similarly to approx_sum. approx_avg doesn't have the second parameter for sampleWeight.

	The result of running approx_sum, approx_count and approx_avg, is a string that shows the absolute result and confidence region in form: 
		123141 +- 3214 (99% confidence)
	By saying "99% confidence", it means that 99% of the change the precise value of the sum of quantity in tableX will stay in the region.

2.3 Query rewrite

	There is query rewrite when you create and do approximate query on sample tables. The code for Query rewrite is in shell/blinkdb_shell.py. blinkdb_shell.py is a child of impala_shell.py and intercept queries.

2.3.1 Rewrite create sample table query

	Sample table has an extra column sampleWeight than the original table. This column is added in rewrite stage. 

	So we change query:
		$ CREATE TABLE sampleTableX AS SELECT * FROM tableX WHERE randomSampleWith(0.01);
	to 
		$ CREATE TABLE sampleTableX AS SELECT *, 1 / 0.01 as sampleWeight FROM tableX WHERE randomSampleWith(0.01);

2.3.2 Rewrite query sample table

    approx_sum and approx_count requires sampleWeight as its second parameter. So sampleWeight is added.

    So we change query:
        $ SELECT approx_avg(quantity) FROM sampleTableX
    to
        $ SELECT approx_count(quantity) FROM sampleTableX
