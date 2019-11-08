\c 520 500
if [(count .z.x) < 1;
	show `$"usage: q file_str_loader.q string.csv destfile
		where inputfile and destfile are absolute or relative paths to 
		the files expressed as C:/path/file.csv or ../marketdata/t.  The script
		will create a kdb database (destfile) based from the inputfile with the following
		fields... Date, Ticker, Field, Value with the Value field being converted to a float and
		any string values will show up as null values.";
	exit 1
   ]
f1: hsym `$.z.x[0]
f2: hsym `$.z.x[1]
columns: `DATE`TICKER`FIELD`VALUE
if [() ~ key f1; show ("Input file '",.z.x[0],"' not found");exit 1]
x: .Q.fsn[{f2 upsert flip columns!("DSS*D";",")0:x};f1;4194000]
show ("loaded ",(string x)," characters into the kdb database")
exit 0