
package entry ;

import java.io.IOException ;
import org.apache.log4j.Level ;
import datariv.csv.processor.InOut ;
import datariv.csv.runnerquery.Utils ;
import datariv.csv.processor.Processor ;
import datariv.owl.ntriple.OwlToNTripleConverter ;

/**
 *
 * @author ryahiaoui
 */

public class Main {
    
    public static void main(String[] args) throws IOException, Exception {
        
        final String EXTENSION = ".ttl"   ;
        
        String   owl           = null     ;
        String   obda          = null     ;
        String   out           = null     ;
        String   csvDirectory  = null     ;
        String   csvDelemiter  = ";"      ; // Default CSV Separator 
        Level    level         = null     ;
      
        boolean  parallel      = false    ; // Parallel Disabled By Default 
        
        int      fragmentFile  = 0        ; // No Fragmentation File By Default
        int      limPageSize   = 10_000   ; // DEFAULT LIMIT QUERY = 10_000
        int      flushCount    = 10_0000  ; // FLUSH EACH 10_0000 Datas in Memory
       
        boolean  debug         = false    ;
        
        
        for ( int i = 0 ; i < args.length ; i++ )  {
            
            String token = args[i]     ;
           
            switch ( token ) {
                
              case "-owl"            : owl          = args[i+1]  ;
                                       break ;
              case "-obda"           : obda         = args[i+1]  ;
                                       break ;
              case "-out"            : out          = args[i+1]  ;
                                       break ;
              case "-csv_directory"  : csvDirectory = args[i+1]  ;  
                                       break ;
              case "-csv_delemiter"  : csvDelemiter =  args[i+1] ;
                                       break ;
              case "-par"            : parallel     = true       ;
                                       break ;
              case "-debug"          : debug        = true       ;
                                       break ;
              case "-fragment"       : fragmentFile = validate ( Integer.parseInt ( args[i+1] ) ) ;
                                       break ;
              case "-page_size"      : limPageSize  = validate ( Integer.parseInt ( args[i+1] ) ) ;
                                       break ;
              case "-fluch_count"    : flushCount   = validate ( Integer.parseInt ( args[i+1] ) ) ;
                                       break ;
              case "-log_level"      : level        = checkLog ( args[i+1] )                      ;
                                       break ;
            }
        }
               
        String outPath   = out                      ;
        String fileName  = InOut.getfileName( out ) ;
        String directory = InOut.getFolder(out )    ;
        
        if ( ! InOut.isDirectory( out ) ) {
          if ( ! fileName.endsWith( EXTENSION ) ) fileName += EXTENSION          ;
          outPath = directory +  System.getProperty("file.separator") + fileName ; 
        } else {
          directory = outPath.endsWith("/") ? 
                      outPath.substring(0, outPath.length() - 1 )   : 
                      outPath                                       ;
          if( outPath.endsWith("/")) outPath += "data" + EXTENSION  ;
          else  outPath  +=  "/data"  +  EXTENSION                  ;
        }

        /** Convert OWL TO NTriples **/
        
        if ( owl != null ) {
            
           String ontoNameWithExtension    = InOut.getfileName(owl )                                ;
           String ontoNameWithoutExtension = InOut.getFileWithoutExtension( ontoNameWithExtension ) ;
           OwlToNTripleConverter.convert( owl , directory                            + 
                                                System.getProperty("file.separator") +
                                                ontoNameWithoutExtension + ".ttl" )  ;
        }
        
        if( obda != null && ! obda.isEmpty() ) {
            
            String commandPath = Utils.extractCommandQuery() ;

            Processor.Process( commandPath     ,
                               obda            ,
                               csvDelemiter    ,
                               outPath         ,
                               limPageSize     ,
                               fragmentFile    , 
                               flushCount      ,
                               parallel     )  ;


           InOut.rm( commandPath )             ;
           
        } else {
         
            System.out.print(" \n OBDA PATH CAN'T BE NULL OR EMPTY ! \n " ) ;
        }
    }
    
    private static Level checkLog( String level )        {
     
        try {
             return  Level.toLevel(level.toUpperCase() ) ;
        } catch( Exception ex )  {
            System.out.println(" Error : The Level "
                               + " [" + level +"] deosn't exists."  ) ;
            System.out.println(" Retained LEVEL : OFF             " ) ;
            System.out.println("                                  " ) ;
             return Level.OFF                                         ;
        }
    }
    
     private static int validate ( int value ) {
       
        if( value < 0 ) {
           System.out.println( " Values can't be Negatif !! " ) ;
           System.out.println( "                            " ) ;
           System.exit ( 0 )                                    ;
        }
        return value ;
    }
}
