1:Dimension Data

Currency:
LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd] (qvd);

MasterCalendar:
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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd)
Where	
	DisplayDate >= '2016-01-01' and
    DisplayDate <= '2020-11-30';
    
GeoLocation:
LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd] (qvd);

Product:
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
FROM [lib://Section_7_Data/Product.qvd] (qvd);

ProductSubCategory:
LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd] (qvd);

2: Dashboard Fact Data

/*
	
    Scenario is that a customer can use any currency at purchase time e.g. Cust in Japan used GBP or USD etc	

	Steps: 
    
    1: Load Foreign Exchange to a Temp Table and create a composite key for Date and Currency key

	2: Load WebSales to a Temp table 

	3: Lookup the composite key in Forex temp table to obtain the exchange rate that matches the date and currency keys
       in WebSales

	4: drop forex temp
    
    5: resident load a new WebSales table (no concatenate) from the temp WebSales table
    
    6: Test if the exchange rate is null and set to 1 as this is the home currency USD used in any sales that
       were made using USD  
    
    7: drop the temp websales table
        
*/

ForexTemp:
LOAD
	DateKey & CurrencyKey as DatedCurrencyKey,
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates2016To2020.qvd] (qvd);

preWebSales:
LOAD
    DateKey,
    ProductKey,
    CustomerKey,
    PromotionKey,
    CurrencyKey, 
    lookup('xRate','DatedCurrencyKey',DateKey & CurrencyKey,'ForexTemp') as xRate, 
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

drop table ForexTemp;

NoConcatenate
WebSales:
LOAD
    DateKey,
    ProductKey,
    CustomerKey,
    PromotionKey,
    CurrencyKey, 
    if(IsNull(xRate),1,xRate) as xRate, 
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
resident preWebSales;

drop table preWebSales;



3:Dashboard SCD Data xForm


 /*
	
    SCD Transformation here
    
*/

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
    CustCreateDate,
    if(CustEndDate='NULL','2050-12-31',CustEndDate) as CustEndDate
FROM [lib://Section_7_Data/Customer.qvd] (qvd);

inner join IntervalMatch(OrderDate)
Load Distinct CustCreateDate,CustEndDate
resident Customer;

SaleType:
LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd] (qvd);

inner join IntervalMatch(OrderDate) 
load Distinct TypeStartDate,TypeEndDate
Resident SaleType;













                                                                   













