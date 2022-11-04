/* 

We used Concatenate to add the product table cloumns to our WebSales table. 
But even though we have our product info there, there's no association across the 
record structure.
  
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
Concatenate(Websales)
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
