-- Task 1:
-- load data
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
-- group all records
grouped_all = GROUP loyalty_data ALL;
-- count the total number of tuples
tuple_count = FOREACH grouped_all GENERATE COUNT(loyalty_data) AS total_count;
DUMP tuple_count; 

-- Task 2: 
grouped_by_tier = GROUP loyalty_data BY membership_tier;
-- count the number of records (customers) within each tier group
tier_counts = FOREACH grouped_by_tier GENERATE 
         group AS tier, 
         COUNT(loyalty_data) AS count;
DUMP tier_counts;

-- Task 3: 
-- use STRSPLIT to break the comma-separated string into a tuple 
-- assign names and cast to appropriate types
-- STRSPLIT returns a tuple, access elements using positional notation ($0, $1, etc.)
split_account_summary = FOREACH loyalty_data GENERATE
  customer_id,
  first_name,
  last_name,
  email,
  membership_tier,
  phone_numbers,
  transactions, 
  (long)STRSPLIT(account_summary, ',').$0 AS points,     -- points earned
  (double)STRSPLIT(account_summary, ',').$1 AS total_spend,  -- total spend
  (int)STRSPLIT(account_summary, ',').$2 AS purchases,    -- purchases
  (double)STRSPLIT(account_summary, ',').$3 AS lifetime_value; -- lifetime value
STORE split_account_summary INTO 'A2_2025_Q3' USING PigStorage(',');

-- Task 4:
-- sort data by 'lifetime_value' in descending order
sorted_by_ltv = ORDER split_account_summary BY lifetime_value DESC;
-- limit result to top 5 records
top_5_ltv = LIMIT sorted_by_ltv 5;
top_5_output_fields = FOREACH top_5_ltv GENERATE
  first_name,
  last_name,
  membership_tier,
  lifetime_value;
STORE top_5_output_fields INTO 'A2_2025_Q4' USING PigStorage(',');
DUMP top_5_output_fields;


-- Task 5:
-- group by 'membership_tier' 
grouped_tier_for_avg_spend = GROUP split_account_summary BY membership_tier;
-- calculate using AVG()
avg_spend_by_tier = FOREACH grouped_tier_for_avg_spend GENERATE 
            group AS tier, 
            AVG(split_account_summary.total_spend) AS average_spend;
STORE avg_spend_by_tier INTO 'A2_2025_Q5' USING PigStorage(',');
DUMP avg_spend_by_tier;


-- Task 6:
-- use TOKENIZE to split the 'phone_numbers' string by comma into a bag of phone numbers
-- use SIZE to count the number of elements (phones) in the bag
-- filter records where the size of the phone number bag is greater than 1
multiple_phones = FILTER split_account_summary BY SIZE(TOKENIZE(phone_numbers, ',')) > 1;
multiple_phones_output = FOREACH multiple_phones GENERATE
  customer_id,
  first_name,
  last_name,
  phone_numbers;
STORE multiple_phones_output INTO 'A2_2025_Q6' USING PigStorage(',');
DUMP multiple_phones_output; 

-- Task 7:
-- group by 'membership_tier'
grouped_tier_for_sum_points = GROUP split_account_summary BY membership_tier;
-- calculate the sum using SUM()
sum_points_by_tier = FOREACH grouped_tier_for_sum_points GENERATE 
            group AS tier, 
            SUM(split_account_summary.points) AS total_points;
STORE sum_points_by_tier INTO 'A2_2025_Q7' USING PigStorage(',');
DUMP sum_points_by_tier;

-- Task 8:
-- extract the domain part of the email using STRSPLIT with '@'
-- the domain is the second element ($1) of the resulting tuple
email_domains = FOREACH split_account_summary GENERATE 
          STRSPLIT(email, '@').$1 AS domain;
grouped_by_domain = GROUP email_domains BY domain;
-- count the occurrences of each domain
domain_counts = FOREACH grouped_by_domain GENERATE 
          group AS email_domain, 
          COUNT(email_domains) AS count;
STORE domain_counts INTO 'A2_2025_Q8' USING PigStorage(',');
DUMP domain_counts;

