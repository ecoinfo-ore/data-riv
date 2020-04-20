# data-Riv

Semantic data producer from csvs &amp; databases 


#### Build :

```
   mvn clean install assembly:single 
```

#### Command Exp : 

```
    java -jar  target/dataRiv-1.0-jar-with-dependencies.jar \
         -owl  "/home/user/sample/ontology.owl"             \
         -obda "/home/user/sample/mapping.obda"             \
         -out  "/home/user/sample/out/data.ttl"
```

#### Args :

    -obda          : Optional - Default : Null

    -out           : Optional - Default : Null
    
    -owl           : Optional - Default : Null
  
    -csv_directory : Optional - Default : Null
    
    -csv_separator : Optional - Default : ";"

    -log_level     : Optional - Default : INFO   ( WARN, TRACE, OFF, INFO, FATAL, ERROR, DEBU, ALL )
    
    -page_size     : Optional - Default : 10_000 ( QUERY LIMIT )

    -fragment      : Optional - Default : 0      ( No Fragmentation : Put all Result Query in one file )
 
    -flush_count   : Optional - Default : 10_000
 
    -par           : Optional - Default : Boolean Disable ( Parallel extractions )

    -debug         : Optional - Default : Boolean Disable
 
