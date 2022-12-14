1: Sub Routine (CheckLogFileExists)

sub CheckLogFileExists

/*

    	      
    In this case we'll use the FileSize() function as it will return NULL if the file
    is not present.
        
    If not present we can create an initial file using the inline statement
    
    Nb: We could actually call the subroutine to Write the log file rather than her
        
*/

If 	isnull(FileSize('lib://Section 7/LoaderLog.qvd'))	   then			        // 1: Check if the file exists if not then proceed with the rest of the routine else return

TRACE 'lib://Section 7/LoaderLog.qvd ' & 'does not exist - creating it for you ! ' ;		// 2: Demo TRACE in the output pane
     
LET vEnd = Timestamp(Now());									// 3: Set the Time stamp here for the end of this task write
     
     LoaderLog:											// 4: The Log table 
     Load															
      *												// 5: Inline load the entry for the this file create task 			
     InLine [
     		  TaskName,TaskStart,TaskEnd,TableLoaded,RowsProcessed,DataDate,Comment                 
     		  Loader Log,$(vStart),$(vEnd),N/A,1,N/A,New Log File created	
            ];//format of our values for the individual columns

 	 store LoaderLog into lib://Section 7/LoaderLog.qvd;					// 6: Store the log file somewhere
  
 	 drop table LoaderLog;								       // 7: No further need for the table so drop it
  
    else										       // Else , show file is found
 
	 TRACE 'lib://Section 7/LoaderLog.qvd ' & 'found'	;				// 8: Demo TRACE in the output pane 
 
 	endif;											// 9: The if block needs to end here with endif	
	
end sub ;



2: Sub Routine (CheckDataMaxDate)


sub CheckDataMaxDate(mTable,mColumn)

/*

	IMPORTANT NOTE: 
	--------------
    
    Sub routines should be coded before they are used anywhere
    in the Script else your code will fail with e.g. Semantic Error(s)

   
	INFO: 
    
	This sub routine will check the max date of the transaction
    data ready for us to determine when to load the new data from

    mTable and mColumn are parameters parsed by the CALL - these 
	inform the LOAD which table and column to use this keeps the code generic and
    reusable

	We use $ Expand {as we do in variable expansion} so the script will translate 
    this at execution time!

*/

zTemp:
	Load
     max($(mColumn)) as MaxValue
    Resident
     $(mTable);
         
	let vMaxDataDate = date(peek('MaxValue'),'YYYY-MM-DD') ;

	drop table zTemp;


end sub;


3: Sub Routine (WriteToLogFile)

sub WriteToLogFile(mTask,mTable,mRows,mComment)	// Expect Taskname, table name and row processed count by the call

/*

	When writing new log entries to the log file we need
    to load the file content into the model first and then
    use Inline to add new log entries.

	This is why we checked the environment first to ensure the
    file is there!

*/

LoaderLog:												        //	1: Create Log table in model
	Load												        //  2: Import existing log entries from the
     TaskName,													//     log file (or Database if required)
     TaskStart,
     TaskEnd,
     TableLoaded,
     RowsProcessed,
 	 DataDate,
 	 "Comment"
From 
	[lib://Section 7/LoaderLog.qvd] (qvd);

    LET vEnd = Timestamp(Now());									     // 3: Set the end of processing time stamp 

concatenate (LoaderLog)    
	Load													// 4: Inline load the entry for the this task	
      *		
     InLine [
     TaskName,TaskStart,TaskEnd,TableLoaded,RowsProcessed,DataDate,Comment
     $(mTask),$(vStart),$(vEnd),$(mTable),$(mRows),$(vMaxDataDate),$(mComment)
     		];
            
 	 store LoaderLog into lib://Section 7/LoaderLog.qvd (QVD);						// 5: Store the model table to the log file	
     
     drop table LoaderLog;

end Sub;



4: Sub Routine (GetLogDataDate)

sub GetLogDataDate(mTable)												// Expect table name 




	/*Establish the last data date posted to the log file*/



zTemp:														    //	1: Create temp table in model
	Load													   //  2: Import existing log entries from the log file (or Database if required) for specified table
 	   max(DataDate) as MaxValue 
	From 
		[lib://Section 7/LoaderLog.qvd] (qvd) 
	where 
		TableLoaded = '$(mTable)';									   // We use a predicate to get the desired log entry i.e. Max Data Date
    
	let vMaxDataDate = date(peek('MaxValue'),'YYYY-MM-DD');							    // 3: Set the variable with the value of the last loaded data date

	drop table zTemp;

end sub;


5: Sub Routine (CheckBaselineRun)

sub CheckBaselineRun(mTask,mTable); 									// Expect a table name to search for



	/*Here we check if a Baseline load should be done!
    
	If 0 Records returned from the log for the table then
    a baseline is required*/



zTemp:													// 1: Create a temp table to hold the max value
    LOAD												// 2: Load the temp table with the max value
     TaskName,
     TaskStart,
     TaskEnd,
     TableLoaded,
     RowsProcessed,
     DataDate,
     "Comment"
    From 
	  [lib://Section 7/LoaderLog.qvd] (qvd) 
    where 
      TaskName = '$(mTask)' and
      TableLoaded = '$(mTable)';  

      LET vBaseRecords = NoOfRows('zTemp');								// 3: Record the record count in a variable , this is tested in the Baseline sections
 
	  if isnull(vBaseRecords) then
       let vBaseRecords=0 ;
      else 
 	   drop table zTemp; 										// 4: No Need for table so drop it, but test for null variable first									
	  endif;
      
end sub;
