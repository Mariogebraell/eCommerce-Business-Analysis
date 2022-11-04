
/*

	This app is designed as a Model Only app contributing to ...
    
    1: Governed Self Service analytics - Data is consistent and refreshed regularly
    
    2: Users's & BA's need not be concerned with the intricacies of source system
       data extracts and potentially complex rules for processing
       
    3: Avoid the modelling steps to make it easy for re-use and minimal development    
       
    4: The model is easily refreshed on the server and the user can reload their own
       app (in their work stream) to reflect the updated data
    
    5: Can be used in association with Governed meta data e.g. Template apps with master
       item Dimensions, Measures and visualisations thus providing easy drag and drop 
       development for the user

	6: The BA only needs to provide minimal support to the user in as much as a Binary
       Load is to be coded in the user app to import the model
       
*/


Dimension Data: 

/*
	
    Load all dimension tables here first...
    
    Nb: Do not load SCD dimensions here, these will be done during the
        Fact data load in order to process the IntervalMatch

*/

LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd]
(qvd);

LOAD
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates.qvd]
(qvd);

LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd]
(qvd);

LOAD
    DateKey,
    DisplayDate,
    "Day",
    DayAbbr,
    YearWeekNbr,
    "Month",
    MonthAbbr,
    MonthNbr,
    YearNum,
    YearMonth
FROM [lib://Section_7_Data/MasterCalendar.qvd]
(qvd);

LOAD
    ProductKey,
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
FROM [lib://Section_7_Data/Product.qvd]
(qvd);

LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd]
(qvd);

LOAD
    ProductCategoryKey,
    ProductCategoryName
FROM [lib://Section_7_Data/ProductCategory.qvd]
(qvd);



2: Fact & SCD Data

/*

	Load ...
    
    WebSales
    Customer
    SaleType
  

*/

WebSales:
LOAD
    DateKey,
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
FROM [lib://Section_7_Data/WebSales.qvd] (qvd);

Customer:
LOAD
	len(CustEndDate) as CL,
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
//  CountryRegionName,
    CustCreateDate,
    if(CustEndDate='NULL','2050-12-31',CustEndDate) as CustEndDate 
FROM [lib://Section_7_Data/Customer.qvd] (qvd);

inner join IntervalMatch(OrderDate)
Load Distinct CustCreateDate,CustEndDate
resident Customer;

/*

	Exercise 7.11 Solution code 

	Task: Add SaleType as a SCD to the model
    
*/

SaleType:
LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd] (qvd);

inner join IntervalMatch(OrderDate)
load distinct TypeStartDate,TypeEndDate
resident SaleType;




