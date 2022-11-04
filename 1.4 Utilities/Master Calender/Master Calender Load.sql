
Generate Calendar

/*
	Create a master calendar from scratch that will form the basis
    of any app that requires a conformed date structure accross any
    data that utilises a date axis in the analytics

	Steps for processing ...
 
	1: Establish the desired date range Min/Max of transaction date

    2: Establish the type of date key format/value to use 
	    e.g. A number representation of the date 2020-11-30 >> 20201130
 
	3: Declare the 2 variables to hold the Min/Max date range values 
 
               Note: We will need to subtract a day from the Min date to account
		             for the value returned from the function IterNo() which is 1
                     
 	4: Declare the calendar table name
 
	5: Commence the first Load with the 1st row (we have 2 loads to perform)
	    e.g. Write the first data column name and value - DateNum
 	6: Start the second load using the IterNo() function 
 
	    i.e. Calculate the first date value using the Min Date variable and IterNo() and
	         assign it to a field i.e. DateNum
  
	7: Trigger QS to generate a single record that contains the column DateNum and 
	    LOAD it to the table 
 	
    8: And we now need to generate the remainder of the dates in the range by way
	   of a WHILE (Do..loop)
 
	   The WHILE will use same line as the preceding AutoGenerate line but in this
	   case it will test a condition i.e. <= the max date in the range 
 
	9: Store the table to a QVD file for consumption
    
    
*/

  Let vMinDate = NUM('2016-12-28');		
  Let vMaxDate = NUM('2020-11-30');	

  MasterCalendar:											// Create the Calendar table to use in a model
  Load
  	// DateNum,												// You can remove this if required
    Num(Year(DateNum),'0000') & Num(Month(DateNum),'00') & Num(Day(DateNum),'00') as DateKey,
    date(DateNum,'YYYY-MM-DD') as DisplayDate,				// e.g. 2020-12-20    
    Date(DateNum,'WWWW') as Day, 							// e.g. Thursday
    Date(DateNum,'WWW') as DayAbbr,							// e.g. Thu.
    Week(Date(DateNum)) as YearWeekNbr, 					// e.g. 53
    Date(DateNum,'MMMM') as Month,							// e.g. December
    Month(DateNum) as MonthAbbr,							// e.g. Dec.
    Num(Month(DateNum)) as MonthNbr, 						// e.g. 12
    Year(DateNum) as YearNum,								// e.g. 2020
    Year(DateNum) &  Num(Month(DateNum)) as YearMonth;		// e.g. 202012   
  Load														// Load the Calendar 			                                                      
    $(vMinDate) + (IterNo()) as DateNum					    // Set IterNo() 
  AutoGenerate 1											// Qlik Sense will generate the row automatically for us 

  While 													// The while loop will iterate through until the vMaxDate has been reached
    $(vMinDate) + (IterNo()) <= $(vMaxDate);
    
store * from MasterCalendar into 'lib://Section_7_Data/MasterCalendar.qvd' (QVD) ;

drop table MasterCalendar;

exit script;
