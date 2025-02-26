CREATE TABLE IF NOT EXISTS default.fetch_users (
    _id UUID,
    state TEXT DEFAULT NULL,
    createdDate DATETIME DEFAULT NULL,
    lastLogin DATETIME DEFAULT NULL,
    role TEXT DEFAULT NULL,
    active BOOLEAN DEFAULT NULL
) ENGINE = MergeTree()
ORDER BY _id;

CREATE TABLE IF NOT EXISTS default.fetch_brands (
    _id UUID,
    barcode INTEGER DEFAULT NULL,
    brandCode TEXT DEFAULT NULL,
    category TEXT DEFAULT NULL,
    categoryCode TEXT DEFAULT NULL,
    cpg TEXT DEFAULT NULL,
    topBrand TEXT DEFAULT NULL,
    name TEXT DEFAULT NULL
) ENGINE = MergeTree()
ORDER BY _id;

CREATE TABLE IF NOT EXISTS default.fetch_receipts (
    _id UUID,
    bonusPointsEarned DATETIME DEFAULT NULL,
    bonusPointsEarnedReason TEXT DEFAULT NULL,
    createDate DATETIME DEFAULT NULL,
    dateScanned DATETIME DEFAULT NULL,
    finishedDate DATETIME DEFAULT NULL,
    modifyDate DATETIME DEFAULT NULL,
    pointsAwardedDate DATETIME DEFAULT NULL,
    pointsEarned FLOAT DEFAULT NULL,
    purchaseDate DATETIME DEFAULT NULL,
    purchasedItemCount INTEGER DEFAULT NULL,
    rewardsReceiptItemList TEXT DEFAULT NULL,
    rewardsReceiptStatus TEXT DEFAULT NULL,
    totalSpent FLOAT DEFAULT NULL,
    userId UUID
) ENGINE = MergeTree()
ORDER BY _id;