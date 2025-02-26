# fetch

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