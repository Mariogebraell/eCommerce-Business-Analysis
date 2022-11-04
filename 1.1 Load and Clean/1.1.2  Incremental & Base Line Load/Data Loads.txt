1: Check Environment

/*

	Before processing the load we should 
    check that everything exists as expected.

*/

call CheckLogFileExists;




2: Baseline Data

/*

   This section simulates an initial data load from our source data
   we'll store it to the WebSales QVD to facilitate incremental loads
         
   Firstly we check if a Baseline load should be done , not using a Sub Routine in this case
    
*/


call CheckBaselineRun('Base Line Load','WebSales') ;

If  $(vBaseRecords) = 0 then											// 5: Check if no records, if 0 then do an initial load

LIB CONNECT TO 'DESKTOP-2B5BT5A';

WebSales:
SQL SELECT 
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
FROM "eCommerce_ODS".dbo.WebSales;

LET vBaseRecords = NoOfRows('WebSales');

/* 

	Now get the latest date of our transaction data and
    record it to a variable for reuse

*/

call CheckDataMaxDate('WebSales','OrderDate');				                  // 6: Get the max data from our loaded table we'll save it for the next incremental

call WriteToLogFile('Base Line Load','WebSales',vBaseRecords,'Initial Data Load done');	 // 7: Write a log entry to state we did the baseline data 	

STORE WebSales into lib://Section_7_Data/WebSales.qvd (QVD);			        // 8: Save our initial load to a base line QVD, that wa we won't

drop table WebSales;								       // 9: Drop the table as it is no longer needed 

//exit script;

else

call WriteToLogFile('Base Line Load','WebSales',0,'No Baseline required');	    // 10: No need for Baseline so just log that a check took place


endif;


3: Incremental Load

/*

	This section will process new data detected in the source table
    As we are in a simulation then the database is a copy of the original 
    eCommerce_ODS but with a day of new sales data.
    
    Nb: The customer did specify this incremental be performed daily!
    
*/

LIB CONNECT TO 'DESKTOP-2B5BT5A';

call GetLogDataDate('WebSales');				  // 1: Establish which date we'll be taking the source data from

WebSales:						                   // 2: Name a Table for loading													
SQL SELECT 															
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
FROM 
	"eCommerce_ODS_Incremental".dbo.WebSales											
WHERE									//    incremental data extracted from the SQL Table
	OrderDate > '$(vMaxDataDate)';					// 3: Import only the rows where the Order Date is > the previous MaxDate 
    									//	  recording use vMaxDataDate in SQL as the where clause filter 																						// Note, the DBA would work with the DA and possibly put an index on OrderDate to speed up our query
									//       incremental data extracted from the SQL Table
Concatenate																			
LOAD									// 4: Concatenate Load the previous .qvd file to this new incremental data set	 
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
From
	[lib://Section 7/WebSales.qvd] (qvd)
Where
	OrderDate <= '$(vMaxDataDate)';				                                   // 5: Ensure we do not load the same records again so select <= the max date
    
LET vIncRecords = NoOfRows('WebSales');			                                           // 6: Record the row count in a variable for the Log file entry

call WriteToLogFile('Incremental Load','WebSales',vIncRecords,'Processed'); 			  // 7: Write a log entry

store WebSales into lib://Section 7/WebSales.qvd (QVD);					         // 8: Store the refreshed QVD file for consumption by the solution app {Dashboard(s)}

drop table WebSales;



4: Baseline Inventory Data: 


call CheckBaselineRun('Base Line Load','ProductInventory') ;

If  $(vBaseRecords) = 0 then							// 9: Check if no records, if 0 then do an initial load




LIB CONNECT TO 'StockManagement_Incremental';


ProductInventory:
LOAD 										// This code is the same as our original QVD Extract app
	SKU AS ProductKey,
    DateKey,
    UnitCost,
    StockOut,
    date(StockOutDate,'YYYY-MM-DD') as StockOutDate,				// Conform the data format to a consistent format across our data
    StockIn,
    date(StockInDate,'YYYY-MM-DD') as StockInDate,
    StockOnHand,
    StockBackOrderQty,
    date(StockBackOrderDate,'YYYY-MM-DD') as StockBackOrderDate,
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
FROM "C:\USERS\OWNER\ONEDRIVE\DOCUMENTS\QLIK\SENSE\APPS\SECTION 7\Stock Management.accdb\StockManagement_Incremental.accdb".ProductInventory;



LET vBaseRecords = NoOfRows('ProductInventory');



	/*Now get the latest date of our transaction data and
    record it to a variable for reuse*/



call CheckDataMaxDate('ProductInventory','StockOutDate');						          // 6: Get the max data from our loaded table we'll save it for the next incremental

call WriteToLogFile('Base Line Load','ProductInventory',vBaseRecords,'Initial Data Load done');	                 // 7: Write a log entry to state we did the baseline data 	

STORE ProductInventory into lib://Section 7/ProductInventory.qvd (QVD);					        // 8: Save our initial load to a base line QVD, that wa we won't

//drop table ProductInventory;											// 9: Drop the table as it is no longer needed 

//exit script;

else

call WriteToLogFile('Base Line Load','ProductInventory',0,'No Baseline required');				// 10: No need for Baseline so just log that a check took place


endif;




5: Incremental Inventory Data


LIB CONNECT TO 'StockManagement_Incremental';

call GetLogDataDate('ProductInventory');	

let vMaxInvDataDate = date(vMaxDataDate,'DD/MM/YYYY') ;	


ProductInventory:
LOAD 										// This code is the same as our original QVD Extract app
	SKU AS ProductKey,
    DateKey,
    UnitCost,
    StockOut,
    date(StockOutDate,'YYYY-MM-DD') as StockOutDate,				// Conform the data format to a consistent format across our data
    StockIn,
    date(StockInDate,'YYYY-MM-DD') as StockInDate,
    StockOnHand,
    StockBackOrderQty,
    date(StockBackOrderDate,'YYYY-MM-DD') as StockBackOrderDate,
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
FROM "C:\USERS\OWNER\ONEDRIVE\DOCUMENTS\QLIK\SENSE\APPS\SECTION 7\Stock Management.accdb\StockManagement_Incremental.accdb".ProductInventory
WHERE																					
	StockOutDate > #$(vMaxDataDate)#;


Concatenate	
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
From
	[lib://Section 7/ProductInventory.qvd] (qvd)
Where
	StockOutDate <= '$(vMaxDataDate)';	


LET vIncRecords = NoOfRows('ProductInventory');								  // 6:: Record the row count in a variable for the Log file entry

call WriteToLogFile('Incremental Load','ProductInventory',vIncRecords,'Processed'); 			  // 7: Write a log entry

store ProductInventory into lib://Section 7/ProductInventory.qvd (QVD);					 // 8: Store the refreshed QVD file for consumption by the solution app {Dashboard(s)}

drop table ProductInventory;


6: Audit Log View

/* 

  Load the Loader Log seperately to persist a log view without
  the need to run the app again.
 
  This is available for admins to review by opening the app 
  once published on the server 
  
  The log could be expanded as part of the loader process to 
  contain other information such as total sales value for the
  WebSales load as we observed on our sheet
  
  There is endless scope as to what is recorded in log data! 

*/

AuditLog:
LOAD
    TaskName,
    TaskStart,
    TaskEnd,
    TableLoaded,
    RowsProcessed,
    DataDate,
    "Comment"
FROM [lib://Section 7/LoaderLog.qvd] (qvd);


exit script;

