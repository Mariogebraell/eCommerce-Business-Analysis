Step 1 - Unpivot

/*

	1: Load the pivoted data into a staging table from our QVD file 
       generated in the QVD Extract layer

	   Unpivot the staged data using Crosstab (nb: This is a script prefix )   	
    

*/

PivotedForex:
// LOAD
//     DateKey,
//     "Date",
//     AUSTRALIA,
//     EURO,
//     UK,
//     CANADA
// FROM [lib://Section 7/ForeignExchangeRates.qvd] (qvd);
// 																	// Note this is classed a preceding load due to the Pivot data loaded first
                                                                    // ready for a subsequent resident load


LOAD
    DateKey,
    "Date",
    AUSTRALIA,
    EURO,
    UK,
    CANADA
FROM [lib://Section 7/Section_7_Forex_Sample_Data/ForeignExchangeRates.xlsx]
(ooxml, embedded labels, table is ForeignExchangeRates);

LOAD
    DateKey,
    "Date",
    AUSTRALIA,
    EURO,
    UK,
    CANADA
FROM [lib://Section 7/Forex new data - exercise/ForeignExchangeRates_2020.xlsx]
(ooxml, embedded labels, table is ForeignExchangeRates_2020);




ForexRatesHistory:
CrossTable(Country,Rate,2)
Load
	*
resident PivotedForex;

drop table PivotedForex;



Step 2- View Currency Dim

/*
	Step 2: Load the Currency dimension table to profile and 
    		establish a method to obtain the CurrencyKey for the
            ForexRatesHistory table generated in Step 1

			
*/


LOAD
    CurrencyKey,
    CurrencyAlternateKey,
    CurrencyName
FROM [lib://Section 7/Currency.qvd] (qvd);



Step 3 a- Lookup Currency Key

/*
	Step 3: Associate the ForexRatesHistory to Currency dimension via the CurrencyKey
    
    	a: Identify if there is a common key 
           
           If -No- key ; hence we need a candidate column that we can derive the key from so in this case 
           we build a placeholder into the ForexRatesHistory table as a ztemp table and fill it with
           a substring of the country name to get as close as possible to the potential lookup key
*/

zTemp:
Load 
	left(Country,2) as CurrencyAlternateKey,
	*
resident ForexRatesHistory;

drop table ForexRatesHistory;



Step 3b - Lookup Currency Key


/*
	Step 3: Associate the ForexRatesHistory to Currency dimension via the CurrencyKey
    
    	a: Identify if there is a common key 
           
           If -No- key ; hence we need a candidate column that we can derive the key from so in this case 
           we build a placeholder into the ForexRatesHistory table via as a ztemp table and fill it with
           a substring of the country name to get as close as possible to the potential lookup key

		b: Now we have a basic Alternate Key from the string function in 3a , hence we can expand the 
           value based on what we know about the CurrencyAlternateKey and CurrencyName for example
           the string we created for United Kingdom Pound was UK ; hence we can use a nested if() to
           construct the correct CurrencyAlternateKey value for UK and all others too

*/

zTempAltKey:
NoConcatenate Load														// Use NoConcatenate here as QS will merge to the zTemp table as it is the same structure
	if(CurrencyAlternateKey='UK','GBP',									// and duplicate all rows - not ideal
     if(CurrencyAlternateKey='EU','EUR',								
      if(CurrencyAlternateKey='AU','AUD',
       if(CurrencyAlternateKey='CA','CAD')))) as CurrencyAlternateKey,
    DateKey,
    Date,
    Country,
    Rate
resident zTemp;

drop table zTemp;



Step 3c - Lookup Currency Key

/*
	Step 3: Associate the ForexRatesHistory to Currency dimension via the CurrencyKey
    
    	a: Identify if there is a common key 
           
           If No key hence we need a candidate column that we can derive the key from so in this case 
           we build a placeholder into the ForexRatesHistory table via a ztemp table and fill it with
           a substring of the country name to get as close as possible to the potential lookup key

		b: Now we have a basic Alternate Key from the string function in 3a , now we can expand the 
           value based on what we know about the CurrencyAlternateKey and CurrencyName for example
           the string we created for United Kingdom Pound was UK hence we can use a nested if() to
           construct the correct CurrencyAlternateKey value for UK and all others too

		c: Use the Inter-Record function Lookup() to establish the CurrencyKey value to finalise the 
           model, the CurrencyKey will associate to the WebSales Currency Key


*/

ForexRatesHistory:
NoConcatenate Load  							// Use NoConcatenate here as QS will merge to the zTempAltKey table as it is the same structure
     CurrencyAlternateKey,
	 Lookup('CurrencyKey','CurrencyAlternateKey',CurrencyAlternateKey,'ForexRatesHistory-1') as CurrencyKey,
	 DateKey,									
     Date as Closedate,								// Potentially redundant but can be left here for now; no real overhead as above
     Country,						
     Rate as xRate								// Renamed as it was a key word in QS	
resident zTempAltKey;

drop field CurrencyAlternateKey from ForexRatesHistory;
drop table zTempAltKey;

store ForexRatesHistory into lib://Section 7/ForeignExchangeRates.qvd (qvd);

drop table ForexRatesHistory;
