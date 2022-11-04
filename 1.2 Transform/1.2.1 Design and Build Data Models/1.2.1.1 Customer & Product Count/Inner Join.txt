/* 
INNER JOIN

In QLIK SENSE we use join to merge 2 (or more) tables into 1 and when the 
2 tables are merged only the matching keys will be selected

In this example the Product table is joined and merged on matching 
SKUs thus reducing the data set size i.e. row count

See the Sheet to determine this !

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
INNER JOIN
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

