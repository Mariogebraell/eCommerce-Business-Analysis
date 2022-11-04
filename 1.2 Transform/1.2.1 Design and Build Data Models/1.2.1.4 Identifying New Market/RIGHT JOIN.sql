/*
 RIGHT JOIN
 
 In this scenario the user has requested a list of locations
 that indentifies where there is no customer thus providing insight 
 to the marketing research team to review a strategy for greater 
 customer reach across those locations


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
Right Join (Customer)
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
