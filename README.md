# usa-elec

By processing data from different sources, this project highlights how the electrical consumption of industrial sites around the usa evolves depending on the temperature changes.

Data sources :

	- temperature : forecast.io
	
	- electrical consumption : enernoc.com
	
We retrieve and process these data with several Python scripts to obtain csv files.

Then these csv are loaded in a Hive database with some SQL scripts and the Beeline CLI.


When data loading is ok, the analysis is done with some mapreduce through Spark and the PySpark API.

Finally some data vizualisation has been done with Tableau Software to expose the insights.
