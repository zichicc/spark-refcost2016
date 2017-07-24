# Italian Constitutional Referendum 2016 Data Analysis

### Project Setup (IntelliJ Idea / SBT Shell)
Create a `resources` folder in ./src/main/ and place a semicolon-delimited `csv` file in it, 
which should include a header describing these columns:

``` 
    |-- DESCREGIONE: Categorical
    |-- DESCPROVINCIA: Categorical
    |-- DESCCOMUNE: Categorical
    |-- ELETTORI: Numerical
    |-- ELETTORI_M: Numerical
    |-- VOTANTI: Numerical
    |-- VOTANTI_M: Numerical
    |-- NUMVOTISI: Numerical
    |-- NUMVOTINO: Numerical
    |-- NUMVOTIBIANCHI: Numerical
    |-- NUMVOTINONVALIDI: Numerical
    |-- NUMVOTICONTESTATI: Numerical
```

Categorical fields may be null and numerical ones can be malformed (include chars and forming
a string as a consequence, NaN, empty, negative numbers).

In order to launch the Spark Job, type in the command line:

```
sbt clean
sbt "run csv-file-name.csv"
```

or inside an `sbt` shell:

```
clean 
run csv-file-name.csv
```

The output semicolon-delimited csv file will be saved in `./output/` folder as `"input-file-name"-aggregated.csv` with this schema: 

``` 
    |-- DESCREGIONE: string
    |-- ELETTORI_M: long
    |-- ELETTORI_F: long
    |-- ELETTORI: long
    |-- PERCENTUALEVOTANTI: double
    |-- PERCENTUALEVOTI_SI: double
    |-- PERCENTUALEVOTI_NO: double
    |-- PERCENTUALEVOTI_BNVC: double
```
`PERCENTUALEVOTI_BNVC` percentage includes `VOTIBIANCHI`, `VOTINONVALIDI` and `VOTICONTESTATI`

