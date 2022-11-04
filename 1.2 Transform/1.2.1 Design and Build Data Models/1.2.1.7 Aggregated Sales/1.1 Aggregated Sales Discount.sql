/*

	Scenario:
 
	* The BA requires a data set to analyse a specific product category and time period sales.
    
	* The data set should include the full name of the currency used to purchase the product
      and the aggregated Sales $, Tax & Order count value across the date and product values
      
    * Show the value of the sale (pre Tax) when the discount % is applied   
    
    * Show a WhatIf scenario when a 35% discount applied to the sale value for Big World Importers
    
	Segmentation is based on ...
    
    Selection Criteria: 
    
    a:) Sales data for 2018
    
    b:) Reduce the data set to Category Portable Computers

	Steps :
       
    1: Script the Mapping Load for Currency 
    
	2: Load the source WebSales data (based on criteria Year = 2018) which was processed by the Incremental Load
       script 
      
    3: Implement ApplyMap() for Currency
    
    4: Load the master calendar as we'll include some expanded calendar values in the data set so we 
       do not need to code them on the sheet
	
    5: Load the Product, Product Sub Category and Product Category, we need these to set a where clause	
   
	6: Are there any columns that can be exluded to reduce the grouping requirements in the aggregations ? 
   
    7: Code the aggregations & store the table to a QVD 
    
*/




1: Mapping Load


/*

	We'll use mapping load to replace the Currency key that is held in the
    WebSales table and load the Currency name instead	

	Note: This mapping table is removed from your model once the ApplyMap()
    	  process is completed.

*/

MappingCurrency:
Mapping Load
	CurrencyKey,
    CurrencyName
from [lib://Section_7_Data/Currency.qvd] (qvd);



2: Stage the source QVDs



stagedWebSales:
LOAD
    DateKey,
    ProductKey,
    CustomerKey,
    PromotionKey,
//  CurrencyKey,  
    ApplyMap('MappingCurrency',CurrencyKey) as TransactionCurrency,
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
FROM 
	[lib://Section_7_Data/WebSales.qvd] (qvd) 
Where
	Year(OrderDate) = 2018;

inner join
stagedMasterCalendar:
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

inner join
stagedProduct:
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

inner join
stagedProductSubCategory:
LOAD
    ProductSubcategoryKey,
    ProductSubcategoryName,
    ProductCategoryKey
FROM [lib://Section_7_Data/ProductSubCategory.qvd] (qvd);

inner join 
stagedProductCategory:
LOAD
    ProductCategoryKey,
    ProductCategoryName
FROM [lib://Section_7_Data/ProductCategory.qvd] (qvd) 
Where
	ProductCategoryName='Portable Computers';



3: Aggregations 

/*
	Build the final aggregations using the staged data and then
    output to a QVD

*/

aggregWebSales:
Load 
    DisplayDate,
    MonthAbbr,
    Day,
    ProductName,
    Manufacturer,
    TransactionCurrency,
    count(distinct SalesOrderNumber) as OrderCount,
    sum(SalesAmount) as SalesValue,
    sum(TaxAmt) as TaxValue,
	sum(SalesAmount-(SalesAmount*(UnitPriceDiscountPct/100))) as SaleValueIncDiscount,
	
	sum(if(Manufacturer='Big World Importers',SalesAmount-(SalesAmount*.035))) as SaleValueBWI35Discount

resident stagedWebSales
group By
    DisplayDate,
    MonthAbbr,
    Day,
    ProductName,
    Manufacturer,
    TransactionCurrency;  
    
drop table stagedWebSales;

store aggregWebSales into  [lib://Section_7_Data/AggregWebSalesAnalysis.qvd] (QVD);

drop table aggregWebSales;

exit script;
