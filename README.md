# COVID-19 Loader
A quick way to load the [John Hopkins University compiled COVID-19 case data](https://gisanddata.maps.arcgis.com/apps/opsdashboard/index.html#/bda7594740fd40299423467b48e9ecf6); as well as [World Bank populations by country](https://data.worldbank.org/indicator/SP.POP.TOTL) - to normalise the data against.

##Outputs

1. Two reformatted CSV files with case data and population data; ready for use in viz tools like Tableau
2. A set of Postgres tables with normalised infection rates by country; ready for geospatial viz & analysis
