/* 
LEFT JOIN

In this scenario we are profiling the Customer table data to 
establish which customers do not have the city value applied to their 
record via a GeographyKey association to GeoLocation.


In addition it can be used for 1 off Q&A from your user(s)!

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
    CountryRegionName,
    CustCreateDate,
    CustEndDate
FROM [lib://Section 7/CRM-GeoLocation/Customer.csv]
(txt, codepage is 28591, embedded labels, delimiter is ',', msq);

GeoLocation:
LEFT JOIN
LOAD
    GeographyKey,
    City,
    StateProvinceName,
    CountryRegionName,
    SalesTerritoryKey,
    Latitude,
    Longitude
FROM [lib://Section 7/CRM-GeoLocation/GeoLocation.csv]
(txt, utf8, embedded labels, delimiter is ',', msq);

exit Script;
