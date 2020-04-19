# data-Riv

Semantic data producer from csvs &amp; databases 

#### Args :

    -obda          : Optional - Default : Ignored

    -out           : Optional - Default : Ignored
    
    -owl           : Optional - Default : Ignored
  
    -csv_directory : Optional - Default : Ignored
    
    -csv_separator : Optional - Default : ";"

    -log_level     : Optional - Default : INFO   ( WARN, TRACE, OFF, INFO, FATAL, ERROR, DEBU, ALL )
    
    -page_size     : Optional - Default : 10_000 ( QUERY LIMIT )

    -fragment      : Optional - Default : 0      ( No Fragmentation : Put all Result Query in one file )
 
    -flush_count   : Optional - Default : 10_000
 
    -par           : Optional - Default : Boolean Disable ( Parallel extractions )

    -debug         : Optional - Default : Boolean Disable
 

## Command Exp : 

```
    java -jar  target/dataRiv-1.0-jar-with-dependencies.jar \
         -owl  "/home/ryahiaoui/sample/ontology.owl"        \
         -obda "/home/ryahiaoui/sample/mapping.obda"        \
         -out  "/home/ryahiaoui/sample/out/data.ttl"
```
