/*
 OUTER JOIN
 
 In this scenario the user has requested a list of all customers with 
 no city and all cities with no customers.
 
 The usefulness of this join type is that the user will receive a single output that...
 
 1: lists Customers with no city i.e. GeographyKey missing(demo'd in the left join)
 
 2: Lists the Cities that we have not made sales in 
 
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
Outer Join (Customer)
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
