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


1: Data Rectification

/*
		1: The underlying data will need to be processed country by country else
           a previous day xRate will potentially be wrong e.g. AUSTRALIA picks up CANADA instead
           
        2: Each pass will eliminate each iteration of the gaps etc; hence run as many as needed 
           i.e. 2 days with no xRate will need 2 passes to end up with the xRate being populated
             
	    Steps:
        
        1: Import the raw qvd data and observe in a straight table
        2: Process AUS Rate and set the date range 2016-2019 
        3: Process xTempPass1 rowset , test for the gaps and set a real null value 
           ready to replace with the Previous day rate
        4: Process xTempPass2 rowset and test the null values to replace with 
           previous day xRate   
        5: As we have consecutive nulls Pass 2 only handled the 1st Null
           hence a 3rd Pass is required to take care of this
  
	6: Replicate steps 1 to 4 but now process CANADA  

*/
// AUD processing

xTempPass1:														// Transform incorrect values to True nulls using the Null() function												
LOAD
    Autonumber(DateKey) as sid1,
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate as xRatePre,
    if(xRate=0 or xRate='NULL' or xRate='-' or xRate='',Null(),xRate) as xRate 
FROM 
	[lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd)
Where
	Country='AUSTRALIA' and
    year(CloseDate) >= 2016 and year(CloseDate) <= 2019   ;

xTempPass2:													   // Replace the true nulls with the previous day xRate
Load
    Autonumber(DateKey) as sid2,
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate as xRatePre,
    if(IsNull(xRate),Previous(xRate),xRate) as xRate
resident xTempPass1;

drop table xTempPass1;

ForeignExchangeRates:										// If consecutive days of Nulls then this pass will fix it and
Load														// we'll save it for final storage to the revised QVD 
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate as xRatePre,
    if(IsNull(xRate),Previous(xRate),xRate) as xRate
resident xTempPass2;

drop table xTempPass2;

// CANADA processing

xTempPass1:														// Transform incorrect values to True nulls using the Null() function												
LOAD
    Autonumber(DateKey) as sid1,
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate as xRatePre,
    if(xRate=0 or xRate='NULL' or xRate='-' or xRate='',Null(),xRate) as xRate 
FROM 
	[lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd)
Where
	Country='CANADA' and
    year(CloseDate) >= 2016 and year(CloseDate) <= 2019   ;

xTempPass2:													   // Replace the true nulls with the previous day xRate
Load
    Autonumber(DateKey) as sid2,
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate as xRatePre,
    if(IsNull(xRate),Previous(xRate),xRate) as xRate
resident xTempPass1;

drop table xTempPass1;

ForeignExchangeRates:										// If consecutive days of Nulls then this pass will fix it and
Load														// we'll save it for final storage to the revised QVD 
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate as xRatePre,
    if(IsNull(xRate),Previous(xRate),xRate) as xRate
resident xTempPass2;

drop table xTempPass2;



exit script;


2: Model Load - Data Validation

/*

	Reload the model as a complete model 

    
    
    select
	  sum(SalesAmount) as WebSalesTotal,
	  sum(ExtendedAmount) as ExtendedAmountTotal
    from
	  [dbo].[WebSales]
      
 
   
   select
	sum(SalesAmount) as WebSalesTotal,
	sum(ExtendedAmount) as ExtendedAmountTotal,
	sum(TaxAmt) as TaxTotal,
	sum(Freight) as FreightTotal
   from
	[dbo].[WebSales]   

*/

WebSales:
LOAD
    DateKey,
    ProductKey, 
    DateKey & ProductKey & CurrencyKey as ltKey,
//    DateKey & CurrencyKey as ltcKey,    
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

ProductInventory:
LOAD
    ProductKey,
    DateKey,  
    DateKey & ProductKey as ltKey,    
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
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd);

ForeignExchangeRates:
LOAD
    CurrencyKey,
    DateKey,
    DateKey & CurrencyKey as ltKey,    
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd);

Currency:
LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd] (qvd);

ltSalesInvForex:
Load
	distinct(ltKey) ,
    DateKey,
    ProductKey,
    'WebSales' as dtSource
resident WebSales;

drop fields ProductKey from WebSales;

Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'Inventory' as dtSource
resident ProductInventory;

drop fields DateKey,ProductKey from ProductInventory;

Concatenate (ltSalesInvForex)
load
	distinct(ltKey),
    DateKey,
    CurrencyKey,
    'WebSales' as dtSource
resident WebSales;

drop fields DateKey,CurrencyKey from WebSales;

Concatenate (ltSalesInvForex)
load
	distinct(ltKey),
    DateKey,
    CurrencyKey,
    'Forex' as dtSource
resident ForeignExchangeRates;
    
drop fields DateKey,CurrencyKey from ForeignExchangeRates;

/*
	
    Load the dimension tables to complete the model 
    MasterCalendar,SaleType,SalesTargets,Product,ProductSubCategory,
    ProductCategory,Customer,GeoLocation
    
*/

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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd);

LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd]
(qvd);

LOAD
    DateKey,
//    "Year",
//    "Month",
    SalesTarget
FROM [lib://Section_7_Data/SalesTargets.qvd] (qvd);

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

LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd] (qvd);

LOAD
    ProductCategoryKey,
    ProductCategoryName
FROM [lib://Section_7_Data/ProductCategory.qvd] (qvd);

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
//    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section_7_Data/Customer.qvd] (qvd);

LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd] (qvd);


exit script;


3:  Solve the Dupes

/*
	Task:
    
    To find the duplicate customers in the data model and display these to the
    User/Customer to investigate
    
    

*/

WebSales:
LOAD
    DateKey,
    ProductKey, 
    DateKey & ProductKey & CurrencyKey as ltKey,
//    DateKey & CurrencyKey as ltcKey,    
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

ProductInventory:
LOAD
    ProductKey,
    DateKey,  
    DateKey & ProductKey as ltKey,    
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
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd);


ForeignExchangeRates:
LOAD
    CurrencyKey,
    DateKey,
    DateKey & CurrencyKey as ltKey,    
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd);

Currency:
LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd] (qvd);

ltSalesInvForex:
Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'WebSales' as dtSource
resident WebSales;

drop fields ProductKey from WebSales;

Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'Inventory' as dtSource
resident ProductInventory;

drop fields DateKey,ProductKey from ProductInventory;

Concatenate (ltSalesInvForex)
load
	distinct(ltKey),
    DateKey,
    CurrencyKey,
    'WebSales' as dtSource
resident WebSales;

drop fields DateKey,CurrencyKey from WebSales;

Concatenate (ltSalesInvForex)
load
	distinct(ltKey),
    DateKey,
    CurrencyKey,
    'Forex' as dtSource
resident ForeignExchangeRates;
    
drop fields DateKey,CurrencyKey from ForeignExchangeRates;

/*
	
    Load the dimension tables to complete the model 
    MasterCalendar,SaleType,SalesTargets,Product,ProductSubCategory,
    ProductCategory,Customer,GeoLocation
    
*/

LOAD
    SupplierKey,
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
    CountryRegionName as Location
FROM [lib://Section_7_Data/ProductSupplier.qvd] (qvd);


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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd);

LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd]
(qvd);

LOAD
    DateKey,
//    "Year",
//    "Month",
    SalesTarget
FROM [lib://Section_7_Data/SalesTargets.qvd] (qvd);

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

LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd] (qvd);

LOAD
    ProductCategoryKey,
    ProductCategoryName
FROM [lib://Section_7_Data/ProductCategory.qvd] (qvd);

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
//    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section_7_Data/Customer.qvd] (qvd);

loosen table Customer;

LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd] (qvd);


exit script;


4: Solve the Loop

/*
	Task :
    
    	Load the ProductSupplier table to the completed model
        
        a: Observe the loop and resolve it in the code
                
*/

WebSales:
LOAD
    DateKey,
    ProductKey, 
    DateKey & ProductKey & CurrencyKey as ltKey,
//    DateKey & CurrencyKey as ltcKey,    
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

ProductInventory:
LOAD
    ProductKey,
    DateKey,  
    DateKey & ProductKey as ltKey,    
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
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd);


ForeignExchangeRates:
LOAD
    CurrencyKey,
    DateKey,
    DateKey & CurrencyKey as ltKey,    
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd);

Currency:
LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd] (qvd);

ltSalesInvForex:
Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'WebSales' as dtSource
resident WebSales;

drop fields ProductKey from WebSales;

Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'Inventory' as dtSource
resident ProductInventory;

drop fields DateKey,ProductKey from ProductInventory;

Concatenate (ltSalesInvForex)
load
	distinct(ltKey),
    DateKey,
    CurrencyKey,
    'WebSales' as dtSource
resident WebSales;

drop fields DateKey,CurrencyKey from WebSales;

Concatenate (ltSalesInvForex)
load
	distinct(ltKey),
    DateKey,
    CurrencyKey,
    'Forex' as dtSource
resident ForeignExchangeRates;
    
drop fields DateKey,CurrencyKey from ForeignExchangeRates;

/*
	
    Load the dimension tables to complete the model 
    MasterCalendar,SaleType,SalesTargets,Product,ProductSubCategory,
    ProductCategory,Customer,GeoLocation
    
*/

LOAD
    SupplierKey,
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
    CountryRegionName as Location
FROM [lib://Section_7_Data/ProductSupplier.qvd] (qvd);


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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd);

LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd]
(qvd);

LOAD
    DateKey,
//    "Year",
//    "Month",
    SalesTarget
FROM [lib://Section_7_Data/SalesTargets.qvd] (qvd);

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

LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd] (qvd);

LOAD
    ProductCategoryKey,
    ProductCategoryName
FROM [lib://Section_7_Data/ProductCategory.qvd] (qvd);

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
//    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section_7_Data/Customer.qvd] (qvd);

LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd] (qvd);


exit script;

5: Circular Reference

/*

	Rectify the Circular reference issue that revealed itself
    after the new Forex link table was introduced.
	
    The outcome of this exercise is to create a single Link Table to
    handle the Inventory and Forex requirements.
    
    Benefits:
    
    	a) Reduced table count and easily read model
        b) Possible performance gain by reducing table count and concatenating the forex link table
           data to the original link table.
       
	Steps:
    
    	1: Add the CurrencyKey to composite key WebSales.ltKey
	    
        2: Remove the WebSales.ltcKey
		
        3: Rename ForeignExchangeRates.ltcKey to ltKey as we are normalising the keys to a single key

        4: Move the code block for ltSalesInv down to the start of the dimension load as we'll arrange
           the link table code as a single code block 

        5: Rename the link table ltSalesInv to ltSalesInvForex to reflect the content of this table 

        6: Concatenate load link table ltSalesInvForex with WebSales.ltKey,WebSales.DateKey,WebSales.CurrencyKey
        
           Note: rename the dtcSource to dtSource as we are conforming the column name as one and
                 set the value to WebSales as this is the currency key set for WebSales        

        7: Concatenate load link table ltSalesInvForex with ForeignExchangeRates.ltKey,ForeignExchangeRates.DateKey,
        									 				ForeignExchangeRates.CurrencyKey
                                                            
           Note: rename the dtcSource to dtSource as we are conforming the column name as one and
                 set the value to Forex as this is the currency key set for ForeignExchangeRates

		8: Reload and observe the model , sheets 	


*/
WebSales:
LOAD
    DateKey,
    ProductKey, 
    DateKey & ProductKey & CurrencyKey as ltKey,
//    DateKey & CurrencyKey as ltcKey,    
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

ProductInventory:
LOAD
    ProductKey,
    DateKey,  
    DateKey & ProductKey as ltKey,    
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
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd);


ForeignExchangeRates:
LOAD
    CurrencyKey,
    DateKey,
    DateKey & CurrencyKey as ltKey,    
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd);

Currency:
LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd] (qvd);

ltSalesInvForex:
Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'WebSales' as dtSource
resident WebSales;

drop fields ProductKey from WebSales;

Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'Inventory' as dtSource
resident ProductInventory;

drop fields DateKey,ProductKey from ProductInventory;

Concatenate (ltSalesInvForex)
load
	distinct(ltKey),
    DateKey,
    CurrencyKey,
    'WebSales' as dtSource
resident WebSales;

drop fields DateKey,CurrencyKey from WebSales;

Concatenate (ltSalesInvForex)
load
	distinct(ltKey),
    DateKey,
    CurrencyKey,
    'Forex' as dtSource
resident ForeignExchangeRates;
    
drop fields DateKey,CurrencyKey from ForeignExchangeRates;



/*
	
    Load the dimension tables to complete the model 
    MasterCalendar,SaleType,SalesTargets,Product,ProductSubCategory,
    ProductCategory,Customer,GeoLocation
    
*/

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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd);

LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd]
(qvd);

LOAD
    DateKey,
//    "Year",
//    "Month",
    SalesTarget
FROM [lib://Section_7_Data/SalesTargets.qvd] (qvd);

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

LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd] (qvd);

LOAD
    ProductCategoryKey,
    ProductCategoryName
FROM [lib://Section_7_Data/ProductCategory.qvd] (qvd);

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
//    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section_7_Data/Customer.qvd] (qvd);

LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd] (qvd);


exit script;


6: Link Table pre integration

/*
	This is the code after Exercise 8.2 has been integrated into it
    which resulted in a Loop i.e. Circular Reference

	Building a link table so that we do not have to concatenate
    the Inventory data
    
    Note: Link tables are not very efficient most particularly when
    	  the volume of data is ver large.
          
          However link tables can be useful for such things as canonical 
          calendars (Covered in QSBA section)
	
    Steps :
    
    1: Load the fact tables i.e. Those that we do not want to concatenate i.e. WebSales & Inventory

    2: Create a sheet object to display 6 KPI's to use as a baseline measure (Note these now!)
    	a) Sales Total , Min Sale Date , Max Sale Date
        b) Inv Row Count , Min Stock Out Date , Max Stock Out Date
    
    3: Create a key that can be used in the fact tables to associate to i.e. The Link Table
       In this case as ProductKey and DateKey are common we can make a composite key by appending
       these two together.   
       
    4: Load a new Link Table with
    	a) Distinct list of ltKeys,ProductKey,DateKey from WebSales and a static column identifying the source of the key (WebSales)
        b) Drop the ProductKey,DateKey from WebSales       
  
    5: Load the new Link Table with 
    	a) Distinct list of ltKeys,ProductKey,DateKey from ProductInventory and a static column identifying the source of the key (Inventory)
        b) Drop the ProductKey,DateKey from Inventory 

 	6: Compare the Sheet KPI's ! If errors in the code or KPI's not matching , review your code before step 7!
       Nb: We should now adjust the Sheet KPI's to account for the new Link Table  

	7: Load the remainder of the model as required 
    
*/

WebSales:
LOAD
    DateKey,
    ProductKey, 
    DateKey & ProductKey as ltKey,
    DateKey & CurrencyKey as ltcKey,    
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

ProductInventory:
LOAD
    ProductKey,
    DateKey,  
    DateKey & ProductKey as ltKey,    
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
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd);

ltSalesInv:
Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'WebSales' as dtSource
resident WebSales;

drop fields ProductKey from WebSales;

Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'Inventory' as dtSource
resident ProductInventory;

drop fields DateKey,ProductKey from ProductInventory;

ForeignExchangeRates:
LOAD
    CurrencyKey,
    DateKey,
    DateKey & CurrencyKey as ltcKey,    
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd);

Currency:
LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd] (qvd);

ltForex:
load
	distinct(ltcKey),
    DateKey,
    CurrencyKey,
    'WebSales' as dtcSource
resident WebSales;

drop fields DateKey,CurrencyKey from WebSales;

load
	distinct(ltcKey),
    DateKey,
    CurrencyKey,
    'Forex' as dtcSource
resident ForeignExchangeRates;
    
drop fields DateKey,CurrencyKey from ForeignExchangeRates;

/*
	
    Load the dimension tables to complete the model 
    MasterCalendar,SaleType,SalesTargets,Product,ProductSubCategory,
    ProductCategory,Customer,GeoLocation
    
*/

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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd);

LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd]
(qvd);

LOAD
    DateKey,
//    "Year",
//    "Month",
    SalesTarget
FROM [lib://Section_7_Data/SalesTargets.qvd] (qvd);

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

LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd] (qvd);

LOAD
    ProductCategoryKey,
    ProductCategoryName
FROM [lib://Section_7_Data/ProductCategory.qvd] (qvd);

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
//    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section_7_Data/Customer.qvd] (qvd);

LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd] (qvd);


exit script;


Link Table pre integration:
/*
	This is the code before we integrated Exercise 8.2

	Building a link table so that we do not have to concatenate
    the Inventory data
    
    Note: Link tables are not very efficient most particularly when
    	  the volume of data is ver large.
          
          However link tables can be useful for such things as canonical 
          calendars (Covered in QSBA section)
	
    Steps :
    
    1: Load the fact tables i.e. Those that we do not want to concatenate i.e. WebSales & Inventory

    2: Create a sheet object to display 6 KPI's to use as a baseline measure (Note these now!)
    	a) Sales Total , Min Sale Date , Max Sale Date
        b) Inv Row Count , Min Stock Out Date , Max Stock Out Date
    
    3: Create a key that can be used in the fact tables to associate to i.e. The Link Table
       In this case as ProductKey and DateKey are common we can make a composite key by appending
       these two together.   
       
    4: Load a new Link Table with
    	a) Distinct list of ltKeys,ProductKey,DateKey from WebSales and a static column identifying the source of the key (WebSales)
        b) Drop the ProductKey,DateKey from WebSales       
  
    5: Load the new Link Table with 
    	a) Distinct list of ltKeys,ProductKey,DateKey from ProductInventory and a static column identifying the source of the key (Inventory)
        b) Drop the ProductKey,DateKey from Inventory 

 	6: Compare the Sheet KPI's ! If errors in the code or KPI's not matching , review your code before step 7!
       Nb: We should now adjust the Sheet KPI's to account for the new Link Table  

	7: Load the remainder of the model as required 
    

*/

WebSales:
LOAD
    DateKey,
    ProductKey, 
    DateKey & ProductKey as ltKey,
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

ProductInventory:
LOAD
    ProductKey,
    DateKey,  
    DateKey & ProductKey as ltKey,    
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
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd);

ltSalesInv:
Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'WebSales' as dtSource
resident WebSales;

drop fields ProductKey from WebSales;

Load
	distinct(ltKey),
    DateKey,
    ProductKey,
    'Inventory' as dtSource
resident ProductInventory;

drop fields DateKey,ProductKey from ProductInventory;

/*
	
    Load the dimension tables to complete the model 
    MasterCalendar,SaleType,SalesTargets,Product,ProductSubCategory,
    ProductCategory,Customer,GeoLocation
    
*/

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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd);

LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd]
(qvd);

LOAD
    DateKey,
//    "Year",
//    "Month",
    SalesTarget
FROM [lib://Section_7_Data/SalesTargets.qvd] (qvd);

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

LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd] (qvd);

LOAD
    ProductCategoryKey,
    ProductCategoryName
FROM [lib://Section_7_Data/ProductCategory.qvd] (qvd);

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
//    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section_7_Data/Customer.qvd] (qvd);

LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section_7_Data/Geolocation.qvd] (qvd);


exit script;


7: Link Table

/*
		Task:
        
    	a) Build a link table to remove the need to concatenate foreign exchange rates into WebSales
        b) Test using a sheet object(s)
	 
        Steps:
        
        1) Load the WebSales,ForeignExchangeRates, Currency
        
        2) Add 1 or more test objects to validate your loaded data , review & snapshot the results 
 
       3) Build the WebSales composite key using DateKey and CurrencyKey columns 
           Note: the key name needs to be unique as we already used a ltKey earlier
                 if you don't then you'll get synth keys on the ltKey  
       
        4) Build the Forex composite key using DateKey and CurrencyKey columns 
        
		5) Test load , observe no errors

        6) Create an ltForex table and load with WebSales.ltcKey , DateKey , CurrencyKey
           
           a) Create static column dtcSource (do not use dtSource again!) with value 'WebSales'        
           b) Drop DateKey , CurrencyKey from WebSales 

        7) Test load , observe no errors

	    8) Load the ltForex table with ForeignExchangeRates.ltcKey , DateKey , CurrencyKey	
 
           a) Create static column dtcSource (do not use dtSource again!) with value 'Forex'        
           b) Drop DateKey , CurrencyKey from ForeignExchangeRates 

*/

WebSales:
LOAD
    DateKey,
    ProductKey, 
    CustomerKey,
    PromotionKey,
    DateKey & CurrencyKey as ltcKey,
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

ForeignExchangeRates:
LOAD
    CurrencyKey,
    DateKey,
    DateKey & CurrencyKey as ltcKey,    
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd);

Currency:
LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd] (qvd);

ltForex:
load
	distinct(ltcKey),
    DateKey,
    CurrencyKey,
    'WebSales' as dtcSource
resident WebSales;
    
drop fields DateKey,CurrencyKey from WebSales;

load
	distinct(ltcKey),
    DateKey,
    CurrencyKey,
    'Forex' as dtcSource
resident ForeignExchangeRates;
    
drop fields DateKey,CurrencyKey from ForeignExchangeRates;

exit script;


8:Synthetic Keys

/*
	
    Iterative load to test the model is properly supported.
    
    ** Rectify issues along the way **
   
    1: MasterCalendar 
    2: WebSales
    3: SaleType (Ignore SCD at this point)
    4: SalesTargets
    
	5: Load now and view model 

    6: Product
    7: ProductSubCategory
    8: ProductCategory
    
    9: Load now and view model

    10: Customer 
    11: Geolocation
    
    12: Load now and view model

    13: Inventory load and view model

*/

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
FROM [lib://Section_7_Data/WebSales.qvd]
(qvd);

LOAD
    SaleTypeKey,
    SaleTypeName,
    SaleTypeCategory,
    TypeStartDate,
    TypeEndDate
FROM [lib://Section_7_Data/SaleType.qvd]
(qvd);

LOAD
    DateKey,
//    "Year",
//    "Month",
    SalesTarget
FROM [lib://Section_7_Data/SalesTargets.qvd]
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
//    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section_7_Data/Customer.qvd]
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

concatenate (WebSales)
LOAD
    ProductKey,
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
FROM [lib://Section_7_Data/ProductInventory.qvd]
(qvd);


9: Forex,Currency

    Load 
    
    * Foreign Exchange Rates (QVD file source)
 	* Currency (QVD file source)

	Identify the Synth Keys and decide/implement the fix
    
    14: Forex 
    15: Currency 
    */


concatenate (WebSales)
LOAD
    CurrencyKey,
    DateKey,
    CloseDate,
    Country,
    xRate
FROM [lib://Section_7_Data/ForeignExchangeRates.qvd] (qvd);


LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section_7_Data/Currency.qvd] (qvd);
