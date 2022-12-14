
1:Dimension Data

/*
	
    Load all dimension tables here first that do not require the fact table(s) ...

*/

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





-- Analysis & Reporting Load --


1: Variables  (Analysis)

/* 

	Maintain a naming standard in your Variables so you can
    assimilate to what they do

*/


let condMapCalc 	= 1 ;			// Conditional calc indicator for Map

let sldPriceAdd 	= 1 ;      		// Slider variable for setting the Test price affecting Sales & Profit
let sldPDiscount    = 1 ;      		// Slider variable for setting the Test Sales & Profit by a discounted amount 

let saOrdered		= 'DateType={"Ordered"}'   	;   // Used in all normal aggregation expressions 
let saScheduled		= 'DateType={"Scheduled"}'  ;   // Used in all expressions looking to count by Scheduled
let saShipped 		= 'DateType={"Shipped"}'    ;   // Used in all expressions looking to count by Shipped


2:Dimensions Data


Targets:
LOAD
    DateKey,
//     "Year",
//     "Month",
    SalesTarget
FROM [lib://Section_7_Data/SalesTargets.qvd] (qvd);


3: Inventory Fact Data

/*
	
    Inventory data can be concatenated to WebSales

	i.   Concatenate the table to WebSales 
    
    ii.  Set a numeric flag for Overstocked products (using numerics is more performant in aggregations) and
         Set a message as 'Over Stocked' because this is what a user will see on a sheet
         
    iiv. Set a numeric flag for Out of Stock products (using numerics is more performant in aggregations) and
         Set a message as 'Out of Stock' because this is what a user will see on a sheet
         
    iv. Set a numeric flag for Understocked products (using numerics is more performant in aggregations) and
        Set a message as 'Under Stocked' because this is what a user will see on a sheet

    v.  Set a numeric flag for levelled ok stocked products (using numerics is more performant in aggregations) and
        Set a message as 'Stock Lvl OK' because this is what a user will see on a sheet
        
*/

Concatenate (WebSales)
LOAD
	DateKey & ProductKey & 0 as DateBridgeKey ,
    'Stock' as SourceTable,
	ProductKey,
    DateKey,  
//  UnitCost,
//  StockOut,
//  StockOutDate,
//  StockIn,
//  StockInDate,
    StockOnHand,	// Non aggregatable over time as this is a snapshot of position 

	// Set flags for reporting, best done in loader as this is more performant than
    // testing at calculation i.e. SA level
    
    if(StockOnHand>MaxStockLevel,1) as flagOverStocked,
    if(StockOnHand>MaxStockLevel,'Over Stocked') as msgOverStocked,  
    
    if(StockOnHand=0,1) as flagOutOfStock,    
    if(StockOnHand=0,'Out of Stock') as msgOutOfStock,     

	if(StockOnHand>0 and StockOnHand<MinStockLevel,1) as flagUnderStocked,    
    if(StockOnHand>0 and StockOnHand<MinStockLevel,'Under Stocked') as msgUnderStocked, 
    
    // Exercise 12.2 student coded this status
    
    if(StockOnHand>=MinStockLevel and  StockOnHand<=MaxStockLevel,1) as flagStockLevelOK,
    if(StockOnHand>=MinStockLevel and  StockOnHand<=MaxStockLevel,'Stock Lvl OK') as msgStockLevelOK,    
	
//  StockBackOrderQty,
//  StockBackOrderDate,
    MinStockLevel,
    MaxStockLevel
//  StockTakeFlag
	
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd);


4: Canonical Calendars

/*

	Canonical calendar allows metrics within disparate date(s) of a time line to be 
    aligned to a common timeline.
    
*/

 
DateBridge:
Load
	distinct(DateBridgeKey),
    DateKey,
    'Stock' as DateType
resident WebSales where SourceTable='Stock';

Load
	distinct(DateBridgeKey),
    DateKey,
    'Ordered' as DateType
resident WebSales where SourceTable='WebSales';

Load
	distinct(DateBridgeKey),
    num(year(DueDate),'0000') & num(month(DueDate),'00') & num(day(DueDate),'00') as DateKey,
    'Scheduled' as DateType
resident WebSales where SourceTable='WebSales';
    
// Exercise 13.6

Load
	distinct(DateBridgeKey),
    num(year(ShipDate),'0000') & num(month(ShipDate),'00') & num(day(ShipDate),'00') as DateKey,
    'Shipped' as DateType
resident WebSales where SourceTable='WebSales';

drop field DateKey,SourceTable from WebSales;




                                                                   













