# Hbase TPCC Benchmark

Hbase assignment on the TPCC benchmark.

# IMPORTANT NOTES

We decided to test some region splitting on the customer table. To do so we have created a "regular" customer
table without splitting and a "splitted" customer table called customer_splitted.

The splitted tables has been splitted in 28 region. Each region contain a tuple (warehouse,district), given the nature of the getkey function the tuple (W_ID,1) and (W_ID,10) belongs to the same region.

WE ADDED ONE QUERY!
which is query5 and is exactly as query 4 but operates on the "customer_splitted" table.

The query4 can be run from CLI in the same way as query4.

We added some printout of the walltime to see if there is any speedup, but we where not able to record any.
We think that the file is too small to have an impact on the full scan needed by the filter.
