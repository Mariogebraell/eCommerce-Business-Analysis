/*

	Scenario:
    
    * The BA requires a data set to analyse overstocked inventory for 2019 

    * In the Products table the FinishedGoodsFlag should be substituted as ...

        0 = Unfinished

        1 = Finished

    * Note there is no table data for this substition hence you will need to use
          the INLINE as part of the Mapping code.


    * The BA requires for the data set to include... 

        a) Count of over stocked products
            Business rule is : StockOnHand > MaxStockLevel

        b) Cost of over stocked products
            Business rule is : if StockOnHand > MaxStockLevel then UnitCost * StockOnHand-MaxStockLevel

	Steps :
      
    1: Script the Mapping Load for FinishedGoodsFlag 
    
	2: Load the source ProductInventory data (based on criteria Year = 2019) which was processed by 
       the Incremental Load script 
      
    3: Load Products and Implement ApplyMap() for FinishedGoodsFlag
    
    4: Load the master calendar as we'll include some expanded calendar values in the data set so we 
       do not need to code them on the sheet
	  
	5: Are there any columns that can be exluded to reduce the grouping requirements in the aggregations ? 
   
    6: Code the aggregations & store the table to a QVD   
    
*/


7.10 1: Mapping Load

/*

	We'll use mapping load to replace the Finished Goods flag that is held in the
    Product table and load a staus for Finished Goods instead	

	In this case we will produce a table as one does not exist in our data set
    to do this we'll use the INLINE LOAD 
    
*/

MappingProducts:
 Mapping Load
 	FinishedGoodsFlag,
    FGStatusName
 Inline
 	[FinishedGoodsFlag,FGStatusName
     0,Unfinished
     1,Finished   
    ];





7.10 2: Stage the source QVDs



stagedProductInventory:
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
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd)
Where
	Year(StockOutDate) = 2019;

Inner Join
stagedProduct:
LOAD
    ProductKey,
    ProductSubcategoryKey,
    ProductName,
    StandardCost,
    ApplyMap('MappingProducts',FinishedGoodsFlag) as FGStatusName,
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

Inner Join
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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd);


7.10 2: Stage the source QVDs

/*

	Scenario:
    
    A Data extraction requirement has been emailed to you the DA from the BA !

    * The BA requires a data set to analyse overstocked inventory for 2019 

    * In the Products table the FinishedGoodsFlag should be substituted as ...

        0 = Unfinished

        1 = Finished

    * Note there is no table data for this substition hence you will need to use
          the INLINE as part of the Mapping code.

    * Stage your data as demonstrated in the previous lecture

    * The BA requires for the data set to include... 

        a) Count of over stocked products
            Business rule is : StockOnHand > MaxStockLevel

        b) Cost of over stocked products
            Business rule is : if StockOnHand > MaxStockLevel then UnitCost * StockOnHand-MaxStockLevel

	Steps :
      
    1: Script the Mapping Load for FinishedGoodsFlag 
    
	2: Load the source ProductInventory data (based on criteria Year = 2019) which was processed by 
       the Incremental Load script 
      
    3: Load Products and Implement ApplyMap() for FinishedGoodsFlag
    
    4: Load the master calendar as we'll include some expanded calendar values in the data set so we 
       do not need to code them on the sheet
	  
	5: Are there any columns that can be exluded to reduce the grouping requirements in the aggregations ? 
   
    6: Code the aggregations & store the table to a QVD   
    
*/

stagedProductInventory:
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
FROM [lib://Section_7_Data/ProductInventory.qvd] (qvd)
Where
	Year(StockOutDate) = 2019;

Inner Join
stagedProduct:
LOAD
    ProductKey,
    ProductSubcategoryKey,
    ProductName,
    StandardCost,
    ApplyMap('MappingProducts',FinishedGoodsFlag) as FGStatusName,
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

Inner Join
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
FROM [lib://Section_7_Data/MasterCalendar.qvd] (qvd);


7.10 3: Aggregations

/*

	Process the aggregations and output to a QVD

*/

aggregInventory:
Load 
    DisplayDate,
    YearNum,
    MonthAbbr,
    Day,
    ProductName,
    FGStatusName,
    StockOnHand,
    MinStockLevel,
    MaxStockLevel,
    UnitCost,    
    count(if(StockOnHand>MaxStockLevel,ProductKey)) as OverstockedProductCount,    
    sum(if(StockOnHand>MaxStockLevel,UnitCost*(StockOnHand-MaxStockLevel))) as OverstockedProductCost
resident stagedProductInventory
group by
    DisplayDate,
    YearNum,
    MonthAbbr,
    Day,
    ProductName,
    FGStatusName,
    StockOnHand,
    MinStockLevel,
    MaxStockLevel,
    UnitCost;

drop table stagedProductInventory;

store aggregInventory into  [lib://Section_7_Data/aggregInventoryStockAnalysis.qvd] (QVD);



exit script;



