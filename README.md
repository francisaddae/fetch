# fetch

![ERD TABLE](/erd/fetch.png)

Tables and Relationships:
fetch_receipts (Fact Table)

Stores transaction details like purchaseDate, totalSpent, pointsEarned, and rewardsReceiptStatus.
Foreign Key: userId → Links to fetch_users (_id).
fetch_users (Dimension Table)

Stores user-related details like state, createdDate, and lastLogin.
Primary Key: _id (Referenced by fetch_receipts.userId).
fetch_brands (Dimension Table)

Stores brand-related details such as brandCode, category, and name.
Primary Key: _id.
Potential Relationship: If products in fetch_receipts contain brand details, a link could be established between fetch_brands and fetch_receipts.rewardsReceiptItemList.

Breakdown of tables for dimension modeling. Use of a star schema is quite resourceful here

users table
![USERS MODELING](/erd/users.png)


brands table
![BRANDS MODELING](/erd/brands.png)


receipts table
![RECEIPTS MODELING](/erd/receipts.png)

## PART 2:  SQL Question

* written in postgresql *

1. What are the top 5 brands by receipts scanned for most recent month?
``` sql
select
	fb."name",
	count(fr._id) as total_scanned_reciepts
from fetch_receipts fr
join fetch_brands fb
	on (fr."rewardsReceiptItemList" ->> 'brandCode' = fb."brandCode"
		or fr."rewardsReceiptItemList" ->> 'barcode' = fb."barcode")
where date_trunc('month', fr."dateScanned") = (select date_trunc('month', MAX(fr."dateScanned")) from fetch_receipts fr)
group by fb."name"
order by 2 desc
limit 5;
```

2. How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
```sql
--select both current and previous months
with months_data as (
	select distinct
		date_trunc('month',  fr."dateScanned") as curr_month,
		date_trunc('month', fr."dateScanned" - interval '1 month') as prev_month
	from fetch_receipts fr
),
prev_month_data as (
	select
		fb."name",
		count(fr._id) as total_scanned_reciepts,
		rank() over(order by count(fr._id) desc) as prev_rank
	from fetch_receipts fr
	join fetch_brands fb
		on (fr."rewardsReceiptItemList" ->> 'brandCode' = fb."brandCode"
			or fr."rewardsReceiptItemList" ->> 'barcode' = fb."barcode")
	where date_trunc('month', fr."dateScanned") = (select prev_month from months_data)
	group by fb."name"
),
curr_month_data as (
	select
		fb."name",
		count(fr._id) as total_scanned_reciepts,
		rank() over(order by count(fr._id) desc) as curr_rank
	from fetch_receipts fr
	join fetch_brands fb
		on (fr."rewardsReceiptItemList" ->> 'brandCode' = fb."brandCode"
			or fr."rewardsReceiptItemList" ->> 'barcode' = fb."barcode")
	where date_trunc('month', fr."dateScanned") = (select curr_month from months_data)
	group by fb."name"
)
select
	coalesce(cmd."name", pmd."name") as brand_name,
	cmd.curr_rank as current_brand_rank,
	pmd.prev_rank as previous_brand_rank
from curr_month_data cmd
full join prev_month_data pmd
	on cmd."name" = pmd."name"
where cmd.curr_rank <= 5 or pmd.prev_rank <= 5
order by coalesce(cmd.curr_rank, pmd.prev_rank);
```


3. When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
```sql
select
	fr."rewardsReceiptStatus",
	avg(fr."totalSpent"::float) as average_spent_per_status
from fetch_receipts fr
where lower(fr."rewardsReceiptStatus") in ('accepted', 'rejected')
group by fr."rewardsReceiptStatus";
```


4. When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
``` sql
select
	fr."rewardsReceiptStatus",
	sum(fr."purchasedItemCount") as average_spent_per_status
from fetch_receipts fr
where lower(fr."rewardsReceiptStatus") in ('accepted', 'rejected')
group by fr."rewardsReceiptStatus";
```


5. Which brand has the most spend among users who were created within the past 6 months?
``` sql
with six_month_users  AS(
	select distinct _id as user_id
	from fetch_users fu
	where fu."createdDate" >= fu."createdDate"::date - interval '6 month'
)
select
	fb."name" as brand_name,
	sum(fr."totalSpent"::float) as total_spent_per_brand_users
from fetch_receipts fr
join fetch_brands fb
	on (fr."rewardsReceiptItemList" ->> 'brandCode' = fb."brandCode"
		or fr."rewardsReceiptItemList" ->> 'barcode' = fb."barcode")
join six_month_users smu
	on fr."userId" = smu.user_id
group by fb."name"
order by 2 desc
limit 1;
```

6. Which brand has the most transactions among users who were created within the past 6 months?
``` sql
with six_month_users  AS(
	select distinct _id as user_id
	from fetch_users fu
	where fu."createdDate" >= fu."createdDate"::date - interval '6 month'
)
select
	fb."name" as brand_name,
	count(fr."_id") as total_spent_per_brand_users
from fetch_receipts fr
join fetch_brands fb
	on (fr."rewardsReceiptItemList" ->> 'brandCode' = fb."brandCode"
		or fr."rewardsReceiptItemList" ->> 'barcode' = fb."barcode")
join six_month_users smu
	on fr."userId" = smu.user_id
group by fb."name"
order by 2 desc
limit 1;
```

## PART 3. DATA QUALITY ISSUE

users table :
    check for missing id
    check for cratedDate
    check if active is null

brands table:
    check for mising brandCode
    check for distinct counts of brandId and brandCode
    breakdown of cpgs column
    check topBrand
    check for any missing brandname

receipts


## PART 4. STAKEHOLDERS COMMUNICATIONS









