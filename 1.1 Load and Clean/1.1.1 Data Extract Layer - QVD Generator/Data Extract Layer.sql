1: Variable

// Example varibles set here

/*

  The SET statement is used for defining script variavbles.
  These can be used for substituting strings, paths, drives, and so on.
  
  */
  
  SET vThisProjectVersion = 'v1.00' ;
  
  /*
  
  THE LET statement is a complement to the set statement, used for defining script variables.
  
  The LET statement in opposition to the set statement, evaluates the expression on the right side of the ' = ' symbol before it is assigned to the variable.
  
  */
  
  
  LET vDefaultFormattedTodaysDate = Today();
  
  LET vReformattedTodaysDate = Date(Today(), 'YYYY/MM/DD');
  
  LET mFreightAmount = 'SUM(Freight)';
  
 // exit Script;



2: Currency

Currency:
LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section 7 Data/Section_7_Forex_Sample_Data/Currency.xlsx]
(ooxml, embedded labels, table is Currency);

Store Currency into lib://Section 7 Data/Currency.qvd (QVD);

DROP TABLE Currency;


3:ForeignExchangeRates

ForeignExchangeRates:
LOAD
    DateKey,
    "Date",
    AUSTRALIA,
    EURO,
    UK,
    CANADA
FROM [lib://Section 7 Data/Section_7_Forex_Sample_Data/ForeignExchangeRates.xlsx]
(ooxml, embedded labels, table is ForeignExchangeRates);

Store ForeignExchangeRates INTO lib://Section 7 Data/ForeignExchangeRates.qvd (QVD);

DROP TABLE ForeignExchangeRates;


4: Web Sales 

NullAsValue ShipDate;
set NullValue = 'Not Shipped';

WebSales:
LOAD DateKey,
    ProductKey,
    CustomerKey,
    PromotionKey,
    CurrencyKey,
    SaleTypeKey,
    OrderDate,
    DueDate,
    ShipDate,
    SalesOrderNumber,
    SalesOrderLineNumber,
    RevisionNumber,
    OrderQuantity,
    UnitPrice,
    UnitPriceDiscountPct,
    ProductStandardCost,
    TotalProductCost,
    SalesAmount,
    TaxAmt,
    Freight,
    ExtendedAmount;
SQL SELECT DateKey,
    ProductKey,
    CustomerKey,
    PromotionKey,
    CurrencyKey,
    SaleTypeKey,
    OrderDate,
    DueDate,
    ShipDate,
    SalesOrderNumber,
    SalesOrderLineNumber,
    RevisionNumber,
    OrderQuantity,
    UnitPrice,
    UnitPriceDiscountPct,
    ProductStandardCost,
    TotalProductCost,
    SalesAmount,
    TaxAmt,
    Freight,
    ExtendedAmount
FROM "eCommerce_ODS".dbo.WebSales;

STORE WebSales INTO lib://Section 7 Data/WebSales.qvd; 

DROP Tables WebSales;


5: Sale Type 

SaleType:
LOAD SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate;
SQL SELECT SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM "eCommerce_ODS".dbo.SaleType;

STORE SaleType INTO lib://Section 7 Data/SaleType.qvd; 
Drop Tables SaleType;

6 : Product Inventory 

ProductInventory:
LIB CONNECT TO 'Stock Management';

LOAD SKU AS ProductKey,
    DateKey,
    UnitCost,
    StockOut,
    StockOutDate,
    StockIn,
    StockInDate,
    StockOnHand,
    StockBackOrderQty,
    StockBackOrderDate,
    MinStockLevel,
    MaxStockLevel,
    StockTakeFlag;
SQL SELECT SKU,
    DateKey,
    UnitCost,
    StockOut,
    StockOutDate,
    StockIn,
    StockInDate,
    StockOnHand,
    StockBackOrderQty,
    StockBackOrderDate,
    MinStockLevel,
    MaxStockLevel,
    StockTakeFlag
FROM "C:\USERS\OWNER\ONEDRIVE\DOCUMENTS\QLIK\SENSE\APPS\SECTION 7\STOCK MANAGEMENT.ACCDB\StockManagement.accdb".ProductInventory;

Store ProductInventory into lib://Section 7 Data/ProductInventory.qvd; 



7:ProductCategory-SubCategory

 ProductCategory:
 LOAD ProductCategoryKey,
    ProductCategoryName;
SQL SELECT ProductCategoryKey,
    ProductCategoryName
FROM "C:\USERS\OWNER\ONEDRIVE\DOCUMENTS\QLIK\SENSE\APPS\SECTION 7\STOCK MANAGEMENT.ACCDB\StockManagement.accdb".ProductCategory;

Store ProductCategory into lib://Section 7 Data/ProductCategory.qvd; 

ProductSubCategory:
LOAD ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey;
SQL SELECT ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM "C:\USERS\OWNER\ONEDRIVE\DOCUMENTS\QLIK\SENSE\APPS\SECTION 7\STOCK MANAGEMENT.ACCDB\StockManagement.accdb".ProductSubcategory;

Store ProductSubCategory into lib://Section 7 Data/ProductSubCategory.qvd; 


8: Product - ProductSupplier

Product:
LOAD SKU AS ProductKey,
    ProductSubcategoryKey,
    ProductName,
    StandardCost,
    FinishedGoodsFlag,
    "Color",
    ReorderPoint,
    ListPrice,
    DaysToManufacture,
    ProductLine,
    DealerPrice,
    Manufacturer,
    Description,
    Status,
    SupplierKey;
SQL SELECT SKU,
    ProductSubcategoryKey,
    ProductName,
    StandardCost,
    FinishedGoodsFlag,
    "Color",
    ReorderPoint,
    ListPrice,
    DaysToManufacture,
    ProductLine,
    DealerPrice,
    Manufacturer,
    Description,
    Status,
    SupplierKey
FROM "C:\USERS\OWNER\ONEDRIVE\DOCUMENTS\QLIK\SENSE\APPS\SECTION 7\STOCK MANAGEMENT.ACCDB\StockManagement.accdb".Product;

Store Product into lib://Section 7 Data/Product.qvd;

ProductSupplier:
LOAD SupplierKey,
    AccountNumber,
    AccountName,
    AverageLeadTime,
    StandardPrice,
    LastReceiptCost,
    LastReceiptDate,
    MinOrderQty,
    MaxOrderQty,
    OnOrderQty,
    UnitMeasureCode,
    ActiveFlag,
    CountryRegionName;
SQL SELECT SupplierKey,
    AccountNumber,
    AccountName,
    AverageLeadTime,
    StandardPrice,
    LastReceiptCost,
    LastReceiptDate,
    MinOrderQty,
    MaxOrderQty,
    OnOrderQty,
    UnitMeasureCode,
    ActiveFlag,
    CountryRegionName
FROM "C:\USERS\OWNER\ONEDRIVE\DOCUMENTS\QLIK\SENSE\APPS\SECTION 7\STOCK MANAGEMENT.ACCDB\StockManagement.accdb".ProductSupplier;

Store ProductSupplier into lib://Section 7 Data/ProductSupplier.qvd;

Drop tables ProductInventory,ProductSupplier,ProductCategory,ProductSubCategory,Product;

9: Customer

Customer:
LOAD
    CustomerKey,
    GeographyKey,
    CustomerAlternateKey,
    Title,
    FirstName,
    MiddleName,
    LastName,
    NameStyle,
    BirthDate,
    MaritalStatus,
    Suffix,
    Gender,
    EmailAddress,
    YearlyIncome,
    TotalChildren,
    NumberChildrenAtHome,
    EducationLevel,
    Occupation,
    HouseOwnerFlag,
    NumberCarsOwned,
    AddressLine1,
    AddressLine2,
    Phone,
    DateFirstPurchase,
    CommuteDistance,
    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section 7 Data/CRM-GeoLocation/Customer.csv]
(txt, codepage is 28591, embedded labels, delimiter is ',', msq);

Store Customer into lib://Section 7 Data/Customer.qvd; 

10: GeoLocation

 GeoLocation:
LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section 7 Data/CRM-GeoLocation/GeoLocation.csv]
(txt, utf8, embedded labels, delimiter is ',', msq);

Store GeoLocation into lib://Section 7 Data/GeoLocation.qvd; 
DROP TABLES GeoLocation,Customer;


11: Sales Target 

SalesTarget:
LOAD
    DateKey,
    "Year",
    "Month",
    SalesTarget
FROM [lib://Section 7 Data/SalesTargets/SalesTargets.xlsx]
(ooxml, embedded labels, table is SalesTargets);

Store SalesTarget into lib://Section 7 Data/SalesTarget.qvd; 

DROP TABLE SalesTarget;
