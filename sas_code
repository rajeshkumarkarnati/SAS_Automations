

options symbolgen mlogic mprint obs=max VALIDVARNAME= UPCASE  SPOOL;

libname sasout '/gpfs/sampldata/pharma/retail/sasout';
x"mv /gpfs/sampldata/pharma/retail/sasout/previous/*.sas7bdat /gpfs/sampldata/pharma/retail/sasout/archive/"
x"mv /gpfs/sampldata/pharma/retail/sasout/*.sas7bdat /gpfs/sampldata/pharma/retail/sasout/previous/"

%sysexec %str(cd /gpfs/sampldata/pharma/retail/flatfiles; ls *.txt > /gpfs/sampldata/pharma/retail/filelst.dat);

proc format;
value $dstname
'Drug_distribution_datasets'='DDD'
'Heathcare_Practier_XREF_F'='ACCT_SSK_CCTRCM_XREF'
'Professional_HCP_Mapping_XREF_F'='CUST_XREF'
'Professional__HCA_Mapping_F'='CUST_HCP_XREF'
...
...
...
;
run;


%macro doit;


data flnmlst;
infile '/gpfs/sampldata/pharma/retail/filelst.dat' truncover ;
input 
     var1 $100.  
;

run;
/****************************************************************/
/* get the list of files avaiable in the path and create datasets */
/****************************************************************/
proc sql;

select distinct count(var1) into :cnt from flnmlst;

select distinct var1 into :flnm1 - :flnm%sysfunc(trim(&cnt.)) from flnmlst;

quit;

%do i=1 %to &cnt.;

/****************************************************************/
/* clear macro variable file reference and delete test dataset */
/****************************************************************/
%let dtsnm=;
FILENAME infl CLEAR;
proc delete data=work.test; run;


/****************************************************/
/* read the file and creat the respetive dataset    */
/****************************************************/
filename infl "/gpfs/sampldata/pharma/retail/flatfiles/&&flnm&i." ;

data test;
length dtsnmv $100.;

/*  get the name to match format */
       nm=substr("&&flnm&i.",1,index("&&flnm&i.", "_Q")-1); 

/* get the qtr and year */ 
      qtrfull=substr("&&flnm&i.",index("&&flnm&i.", "Q"),6); 
      qtr1=substr(qtrfull,1,2);
      yer=substr(qtrfull,length(qtrfull)-1,2);
      curdt = put(today(),mmddyy6.);
      format curdt $6.;
      qtr=cats(qtr1,yer);
     

/* prepare dataset name */


       sascode_nm=put(compress(nm),$dstname.);

       dtsnmv = cats(left(compress(sascode_nm)),'_',qtr,'_',curdt); 

/* create macro varaibles */ 
       call symput('dtsnm',dtsnmv);
       call symput('sascdnm',lowcase(compress(sascode_nm)));
       call symput('qtr',qtr);
       call symput('saprt','_');

run;


proc print data=test; run;

data sasout.&dtsnm. ;

   infile infl dsd dlm='|' truncover lrecl=2000 firstobs=2;    

   input
   
    %inc "/gpfs/sampldata/pharma/retail/&sascdnm..sas";
run;


proc contents data=sasout.&dtsnm. varnum; run;
proc print data=sasout.&dtsnm.(obs=20) ; run;

%end;
%mend doit;
%doit;
x"mv /gpfs/sampldata/pharma/retail/flatfiles/*.txt /gpfs/sampldata/pharma/retail/flatfiles/archive/";
