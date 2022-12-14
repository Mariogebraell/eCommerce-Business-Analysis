/* 
KEEP JOIN

In QLIK SENSE we use join to merge 2 (or more) tables into 1 and when the 
2 tables are merged only the matching keys will be selected

If you wanted the model show the associated tables i.e. a Physical representation
then specifying the KEEP prefix will retain the physical reperesentation of the model

In this example the Product table is joined and reduced on matching 
SKUs thus reducing the data set size i.e. row count but still retains the 2 tables 
associative model.
*/



LIB CONNECT TO 'DESKTOP-2B5BT5A';

Websales:
SQL SELECT DateKey,
    ProductKey AS SKU,
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

LIB CONNECT TO 'Stock Management';

Product:
INNER KEEP
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
