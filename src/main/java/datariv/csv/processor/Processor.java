
package datariv.csv.processor ;

import java.util.Map ;
import java.util.Set ;
import java.util.List ;
import java.util.HashMap ;
import java.io.IOException ;
import java.util.ArrayList ;
import datariv.core.Mapping ;
import java.util.logging.Level ;
import java.util.logging.Logger ;
import datariv.core.ObdaManager ;
import java.util.stream.Collectors ;
import datariv.csv.runnerquery.Utils ;
import datariv.csv.runnerquery.Runner ;
import it.unibz.inf.ontop.model.term.Variable ;
import static datariv.core.ObdaManager.loadOBDA ;

/**
 *
 * @author ryahiaoui
 */

public class Processor {
    
    public static void Process( String  commandPath       ,
                                String  obdaPath          ,
                                String  csvDelemiter      ,
                                String  outPathData       , 
                                int     LIMIT_BATCH_SIZE  ,
                                int     FRAGMENT_FILE     ,
                                int     FLUSH_COUNT       ,
                                boolean parallel          ,
                                String  overrideKeyInOBDA ) throws Exception {

       String  folder                   = InOut.getFolder                ( outPathData ) ;
       String  fileNameWithExtension    = InOut.getfileName              ( outPathData ) ;
       String  extension                = InOut.getFileExtension         ( fileNameWithExtension )  ; 
       String  fileNameWithoutExtension = InOut.getFileWithoutExtension  ( fileNameWithExtension )  ;

       List<MappingAdapter> mappings    = adaptMappings ( loadOBDA( obdaPath, overrideKeyInOBDA ) ) ;
       
       if( parallel ) {
           
               mappings.parallelStream().forEach( mapping ->  {
                   
                   try {
                       Processor.processMapping( mapping                  ,
                                                 commandPath              , 
                                                 csvDelemiter             ,
                                                 LIMIT_BATCH_SIZE         ,
                                                 FRAGMENT_FILE            , 
                                                 FLUSH_COUNT              , 
                                                 folder                   , 
                                                 fileNameWithoutExtension , 
                                                 extension                ) ;
                   } catch (Exception ex) {
                       Logger.getLogger(Processor.class.getName())
                             .log(Level.SEVERE, null, ex )       ;
                   }
               }) ;  
       } else {
           
           for (MappingAdapter mapping : mappings) {
                 
               processMapping( mapping                  , 
                               commandPath              ,
                               csvDelemiter             ,
                               LIMIT_BATCH_SIZE         ,
                               FRAGMENT_FILE            ,
                               FLUSH_COUNT              , 
                               folder                   , 
                               fileNameWithoutExtension , 
                               extension                ) ;
           }
       }
    }
    
    private static void processMapping( MappingAdapter mapping                  ,
                                        String         commandPath              ,
                                        String         csvDelemiter             , 
                                        int            LIMIT_BATCH_SIZE         ,
                                        int            FRAGMENT_FILE            ,
                                        int            FLUSH_COUNT              ,
                                        String         folder                   ,
                                        String         fileNameWithoutExtension ,
                                        String         extension ) throws Exception  {
        
           int    currentPage      = 0 ; 
           double TOTAL_EXTRACTION = 0 ;

           if( LIMIT_BATCH_SIZE >= 0 ) {
                    
               String  id              = mapping.getId()               ;
               String  query           = mapping.getQuery()            ;
               String  tripleMapping   = mapping.getTripleMapping()    ;
                   
               System.out.println("id             = " + id             ) ;
               System.out.println("query          = " + query          ) ;
               System.out.println("tripleMapping  = " + tripleMapping  ) ;
                 
               System.out.println(" Process Node : " + mapping.getId() + " ..." ) ;
                   
               String outPath =  folder + "/" + fileNameWithoutExtension + "_" + id + extension   ; 
                   
               Runner runner  = new Runner( commandPath , query , csvDelemiter )                  ;
                 
               mapping.initLimitOffsetAndOverrideParams( runner.getColumnsNameAsList(), LIMIT_BATCH_SIZE )  ;
                    
                   
               int totalLinesPerTripleMapping = mapping.getTotalLinesPerTripleMapping() ;
                    
               String instanceQuery           = mapping.applyOffset( currentPage ++ )   ;
                   
               System.out.println("");
               System.out.println(" INSTANCE QUERY 0 =========================================");
               System.out.println(instanceQuery);
               System.out.println("===========================================================");
               System.out.println("");
                   
               Map<Integer, List<String>>  resulQuery = runner.runCommandQuery( instanceQuery , false ) ;
          
               System.out.println("===========================================");
               System.out.println("["+tripleMapping +"]");
               System.out.println("===========================================");
                        
               System.out.println("===========================================");
               System.out.println("VARIABLES = " + mapping.getVariablesMapping());
               System.out.println("===========================================");
                        
               System.out.println("===========================================");
               System.out.println("COLUMN NAMES = " + runner.getColumnsName());
               System.out.println("===========================================");
                        
               Map<Integer, String > variablesName =  
                       buildVariableNamesByCOlumnIndexColumnName( mapping.getVariablesMapping() , 
                                                                  runner.getColumnsName()       , 
                                                                  id                         )  ;
                         
               System.out.println("===========================================");
               System.out.println(" VARIABLE NAMES  = " + variablesName);
               System.out.println("===========================================");
                        
               TOTAL_EXTRACTION = applyValuesAndWrite( resulQuery                 , 
                                                       tripleMapping              , 
                                                       variablesName              , 
                                                       outPath                    , 
                                                       totalLinesPerTripleMapping , 
                                                       TOTAL_EXTRACTION           , 
                                                       FRAGMENT_FILE              ,
                                                       FLUSH_COUNT                , 
                                                       extension                ) ;
                        
               if( ! mapping.isAlreadyExistsLimitOffset() ) {
                        
                   while( resulQuery.size() > 0 )           {
                            
                      /**  mapping.getLimit() = LIMIT_BATCH_SIZE  // Overrided   */
                     int  offset    = ( ++currentPage -1 ) *   mapping.getLimit() ; 
                            
                     instanceQuery = mapping.applyOffset( offset )   ;
 
                     System.out.println(" INSTANCE QUERY ==========================================");
                     System.out.println(instanceQuery);
                     System.out.println("===========================================================");
                           
                     resulQuery = runner.runCommandQuery( instanceQuery , false ) ;
                            
                     TOTAL_EXTRACTION = applyValuesAndWrite( resulQuery                 , 
                                                             tripleMapping              , 
                                                             variablesName              ,
                                                             outPath                    ,
                                                             totalLinesPerTripleMapping ,
                                                             TOTAL_EXTRACTION           , 
                                                             FRAGMENT_FILE              , 
                                                             FLUSH_COUNT                , 
                                                             extension                ) ;
                    }
               }
                    
               System.out.println( "\n << Total Extacted Triples For Node [ " + 
                                   id + " ] = "                               + 
                                   TOTAL_EXTRACTION  + " >> \n "           )  ;
            }
    }
     
    private static double applyValuesAndWrite( Map<Integer, List<String>> resulQuery                    ,
                                               String tripleMapping, Map<Integer, String> variablesName , 
                                               String outPath,
                                               int totalLinesPerTripleMapping      ,
                                               double TOTAL_EXTRACTION  , 
                                               int FRAGMENT_FILE , 
                                               int FLUSH_COUNT , 
                                               String extension ) throws IOException {
        
        ArrayList<String> datasToWrite = new ArrayList<>() ;
                 
        int     loopForFlush           =  0               ;
                                                
        for (Map.Entry<Integer, List<String>> entry : resulQuery.entrySet()) {
            
            List<String> queryResultColumn = entry.getValue() ;
              
            String applyedValuesOnOneRow   = applyValuesOnOneRow( queryResultColumn , 
                                                                  tripleMapping     , 
                                                                  variablesName   ) ;

            datasToWrite.add( applyedValuesOnOneRow )       ;
            
            TOTAL_EXTRACTION += totalLinesPerTripleMapping  ;
            loopForFlush     += totalLinesPerTripleMapping  ;
            
            if( FRAGMENT_FILE != 0                     && 
                TOTAL_EXTRACTION % FRAGMENT_FILE == 0  &&
                ! datasToWrite.isEmpty()              ) {
                
                   writeInAppropriateFile( outPath          , 
                                           datasToWrite     , 
                                           extension        , 
                                           TOTAL_EXTRACTION , 
                                           FRAGMENT_FILE )  ;
                   loopForFlush = 0                         ;
           }
            
            else if ( loopForFlush >= FLUSH_COUNT && ! datasToWrite.isEmpty() ) {
                    
                writeInAppropriateFile( outPath          ,
                                        datasToWrite     ,
                                        extension        , 
                                        TOTAL_EXTRACTION ,
                                        FRAGMENT_FILE  ) ;
                loopForFlush  = 0                        ;
                    
           }
        }
        
        if( !datasToWrite.isEmpty() ) {
            
                writeInAppropriateFile( outPath          ,
                                        datasToWrite     ,
                                        extension        , 
                                        TOTAL_EXTRACTION ,
                                        FRAGMENT_FILE  ) ;
                loopForFlush  = 0                        ;
        }
         
        return TOTAL_EXTRACTION ;
    }
   
    private static String applyValuesOnOneRow( List<String>        oneRowColumnsResult  , 
                                               String              tripleMapping        , 
                                               Map<Integer,String> variablesName      ) {
        
        for ( Map.Entry<Integer, String> next : variablesName.entrySet() )              {
            String value  = oneRowColumnsResult.get( next.getKey() )                    ;
            tripleMapping = tripleMapping.replace( "{" + next.getValue() + "}", value ) ;
        }
            
        return tripleMapping ;
    }
    
    private static Map<Integer, String>  buildVariableNamesByCOlumnIndexColumnName( Set <Variable>   variableMapping ,   
                                                                                    Map<Integer, String> columnsName ,
                                                                                    String mappingID               ) {
        Map< Integer, String > variablesName = new HashMap<>() ;
        
        variableMapping.forEach ( var -> {
            int colIndex = Utils.getKeyByValue ( columnsName , var.getName())
                                .findFirst()
                                .orElseThrow( () -> new NotFoundColumnException ( mappingID          ,
                                                                                  var.getName()      ,
                                                                                  columnsName.values() ) ) ;
            variablesName.put( colIndex, var.getName() ) ;
        });
        
        return variablesName ;
    }
   
    private static void writeInAppropriateFile( String       outPath         ,
                                                List<String> datasToWrite    ,
                                                String       extension       ,
                                                double       totalExtraction ,
                                                int          fragmentFile    ) {
        try {
             String out = getNextFile( outPath   , 
                                       extension , 
                                       ( int ) ( fragmentFile == 0 ? 0 : 
                                                 totalExtraction/fragmentFile ) ) ;
            
             InOut.writeTextFile ( out , datasToWrite )  ;
             datasToWrite.clear()                        ;
        } catch (IOException ex) {
            Logger.getLogger( ObdaManager.class.getName())
                                         .log(Level.SEVERE, null, ex )  ;
        }
    }
   
    private static String getNextFile( String outFile, String extension, int fragment ) {
    
      if ( fragment <= 0 ) {  return outFile ; }
      
      return outFile.replace( extension, "")   + 
             "_frag-" + fragment + extension   ;             
   } 
    
    
    private static List<MappingAdapter> adaptMappings( List<Mapping> mappings ) {
       
      return mappings.stream()
                     .map( MappingAdapter::new    )
                     .collect(Collectors.toList() ) ;
   }
   
}
