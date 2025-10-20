
-- Task 1: Load the source data and count total tuples
-- Load data using PigStorage with '|' delimiter and define schema [cite: 325, 348, 1167, 1489, 2904]
loyalty_data = LOAD 'loyalty_data.txt' 
               USING PigStorage('|') 
               AS (customer_id:long, 
                   first_name:chararray, 
                   last_name:chararray, 
                   email:chararray, 
                   membership_tier:chararray, 
                   phone_numbers:chararray,
                   transactions:chararray,   
                   account_summary:chararray); 

-- Group all records into a single group to count them
grouped_all = GROUP loyalty_data ALL;

-- Count the total number of tuples in the loaded data
tuple_count = FOREACH grouped_all GENERATE COUNT(loyalty_data) AS total_count;
DUMP tuple_count; 

-- Task 2: Calculate customers per membership tier
-- Group data by the 'membership_tier' field
grouped_by_tier = GROUP loyalty_data BY membership_tier;
-- Count the number of records (customers) within each tier group [cite: 703, 1332, 1938]
tier_counts = FOREACH grouped_by_tier GENERATE 
                 group AS tier, 
                 COUNT(loyalty_data) AS count;
DUMP tier_counts;
-- Task 3: Split the account_summary field [cite: 1070, 2931]

-- Use STRSPLIT to break the comma-separated string into a tuple 
-- Assign names and cast to appropriate types
-- Note: STRSPLIT returns a tuple, access elements using positional notation ($0, $1, etc.)
split_account_summary = FOREACH loyalty_data GENERATE
    customer_id,
    first_name,
    last_name,
    email,
    membership_tier,
    phone_numbers,
    transactions, 
    (long)STRSPLIT(account_summary, ',').$0 AS points,          -- Points earned
    (double)STRSPLIT(account_summary, ',').$1 AS total_spend,   -- Total spend
    (int)STRSPLIT(account_summary, ',').$2 AS purchases,        -- Purchases
    (double)STRSPLIT(account_summary, ',').$3 AS lifetime_value; -- Lifetime value

-- Store the result into HDFS directory 'A2_2025_Q3' as required
STORE split_account_summary INTO 'A2_2025_Q3' USING PigStorage(',');
---------------------------------------------------------------------------------

-- Task 4: Find top 5 customers by highest lifetime value 
-- Use the relation created in Task 3
-- Sort the data by 'lifetime_value' in descending order
sorted_by_ltv = ORDER split_account_summary BY lifetime_value DESC;

-- Limit the result to the top 5 records
top_5_ltv = LIMIT sorted_by_ltv 5;

-- Select only the required fields for the output
top_5_output_fields = FOREACH top_5_ltv GENERATE
    first_name,
    last_name,
    membership_tier,
    lifetime_value;

-- Store the result into HDFS directory 'A2_2025_Q4' 
STORE top_5_output_fields INTO 'A2_2025_Q4' USING PigStorage(',');
DUMP top_5_output_fields;
-- Task 5: Calculate average total_spend per membership tier
-- Use the relation from Task 3
-- Group by 'membership_tier' again 
grouped_tier_for_avg_spend = GROUP split_account_summary BY membership_tier;

-- Calculate the average of 'total_spend' for each group using AVG()
avg_spend_by_tier = FOREACH grouped_tier_for_avg_spend GENERATE 
                        group AS tier, 
                        AVG(split_account_summary.total_spend) AS average_spend;

-- Store the result into HDFS directory 'A2_2025_Q5'
STORE avg_spend_by_tier INTO 'A2_2025_Q5' USING PigStorage(',');

-- Display the result for Task 5 answer
DUMP avg_spend_by_tier;
-- Task 6: Find customers with more than one phone number

-- Use the relation from Task 3
-- Use TOKENIZE to split the 'phone_numbers' string by comma into a bag of phone numbers
-- Use SIZE to count the number of elements (phones) in the bag
-- Filter records where the size of the phone number bag is greater than 1
multiple_phones = FILTER split_account_summary BY SIZE(TOKENIZE(phone_numbers, ',')) > 1;

-- Project relevant fields for the output
multiple_phones_output = FOREACH multiple_phones GENERATE
    customer_id,
    first_name,
    last_name,
    phone_numbers;

-- Store the result into HDFS directory 'A2_2025_Q6'
STORE multiple_phones_output INTO 'A2_2025_Q6' USING PigStorage(',');
DUMP multiple_phones_output; 

-- Task 7: Sum total points earned per membership tier
-- Use the relation from Task 3
-- Group by 'membership_tier'
grouped_tier_for_sum_points = GROUP split_account_summary BY membership_tier;

-- Calculate the sum of 'points' for each group using SUM()
sum_points_by_tier = FOREACH grouped_tier_for_sum_points GENERATE 
                        group AS tier, 
                        SUM(split_account_summary.points) AS total_points;

-- Store the result into HDFS directory 'A2_2025_Q7'
STORE sum_points_by_tier INTO 'A2_2025_Q7' USING PigStorage(',');
DUMP sum_points_by_tier;

-- Task 8: Count customers per email domain 
-- Use the relation from Task 3
-- Extract the domain part of the email using STRSPLIT with '@'
-- The domain is the second element ($1) of the resulting tuple
email_domains = FOREACH split_account_summary GENERATE 
                   STRSPLIT(email, '@').$1 AS domain;

-- Group by the extracted domain name 
grouped_by_domain = GROUP email_domains BY domain;

-- Count the occurrences of each domain
domain_counts = FOREACH grouped_by_domain GENERATE 
                    group AS email_domain, 
                    COUNT(email_domains) AS count;

-- Store the result into HDFS directory 'A2_2025_Q8'
STORE domain_counts INTO 'A2_2025_Q8' USING PigStorage(',');
DUMP domain_counts;