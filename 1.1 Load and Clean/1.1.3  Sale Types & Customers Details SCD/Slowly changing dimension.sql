1: Sale Type SCD

/*

		Processing the Slowly changing dimension to handle the sales 
        across the different sale types.


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

SaleType:
LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd] (qvd);

inner join IntervalMatch(OrderDate)
Load Distinct TypeStartDate,TypeEndDate
Resident SaleType;

exit script;


2: Customer SCD

/*

	The customer will move around from location to location and as a result the analytics will
    attribute sales to the incorrect locations.
    
    Consequently the Customer SCD dimension contains the CustCreateDate and the CustEndDate
    values.
    
    It is these dates that will ensure the correct sales attribution is applied to the relevant
    location

*/

Geolocation:
LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd] (qvd);

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
Resident Customer;


exit script;
