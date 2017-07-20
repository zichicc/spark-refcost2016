# Italian Constitution Referendum 2016 Data Analysis

Create a `resources` folder in src/main/ and place a semicolon-delimited csv file in it, 
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

The output semicolon-delimited csv file will be saved in `output/` folder with `"input-file-name"-aggregated.csv` 
as name and will have this schema: 

``` 
    |-- DESCREGIONE: string (nullable = true)
    |-- ELETTORI_M: long (nullable = true)
    |-- ELETTORI_F: long (nullable = true)
    |-- ELETTORI: long (nullable = true)
    |-- PERCENTUALEVOTANTI: double (nullable = true)
    |-- PERCENTUALEVOTI_SI: double (nullable = true)
    |-- PERCENTUALEVOTI_NO: double (nullable = true)
    |-- PERCENTUALEVOTI_BNVC: double (nullable = true)
```
`PERCENTUALEVOTI_BNVC` percentage includes `VOTIBIANCHI`, `VOTINONVALIDI` and `NUMVOTICONTESTATI`.

