
package entry ;

import java.io.File ;
import java.util.Objects ;
import java.io.IOException ;
import dataRiv.csv.querier.Utils ;
import dataRiv.csv.processor.InOut ;
import java.io.FileNotFoundException ;
import org.apache.logging.log4j.Level ;
import org.apache.logging.log4j.Logger ;
import dataRiv.csv.processor.Processor ;
import org.apache.logging.log4j.LogManager ;
import dataRiv.owl.ntriples.OwlToNTriplesConverter ;

/**
 *
 * @author ryahiaoui
 */

public class Main {

    private static final Logger LOGGER = LogManager.getLogger(Main.class.getName() ) ;
    
    public static void main(String[] args)     {
    
        final String EXTENSION      = ".ttl"   ;
        
        String   owl                = null     ;
        String   obda               = null     ;
        String   out                = null     ;
        String   csvSeparator       = ";"      ; // Default CSV Separator 
        String   overrideKeyInOBDA  = null     ;
        
        Level    level              = null     ;
      
        boolean  parallel           = false    ; // Parallel Disabled By Default 
        
        int      fragmentFile       = 0        ; // No Fragmentation File By Default
        int      limPageSize        = 10_000   ; // DEFAULT LIMIT QUERY = 10_000
        int      flushCount         = 10_0000  ; // FLUSH EACH 10_0000 Datas in Memory
       
        boolean  debug              = false    ;
        
        for ( int i = 0 ; i < args.length ; i++ )  {
            
            String token = args[i]   ;
           
            switch ( token )         {
                
              case "-owl"            : owl               = args[i+1]  ;
                                       break ;
              case "-obda"           : obda              = args[i+1]  ;
                                       break ;
              case "-out"            : out               = args[i+1]  ;
                                       break ;
              case "-csv_separator"  : csvSeparator      =  args[i+1] ;
                                       break ;
              case "-csv_directory"  : overrideKeyInOBDA =  args[i+1] ;
                                       break ;
              case "-par"            : parallel          = true       ;
                                       break ;
              case "-debug"          : debug             = true       ;
                                       break ;
              case "-fragment"       : fragmentFile      = validate ( Integer.parseInt ( args[i+1] ) ) ;
                                       break ;
              case "-page_size"      : limPageSize       = validate ( Integer.parseInt ( args[i+1] ) ) ;
                                       break ;
              case "-fluch_count"    : flushCount        = validate ( Integer.parseInt ( args[i+1] ) ) ;
                                       break ;
              case "-log_level"      : level             = checkLog ( args[i+1] )                      ;
                                       break ;
            }
        }
 
        Objects.requireNonNull ( out, "Out Can't BE NULL OR EMPTY !  "   ) ;
        
        String outPath   = out                      ;
        String fileName  = InOut.getfileName( out ) ;
        String directory = InOut.getFolder(out )    ;
        
        if ( ! InOut.isDirectory( out ) ) {
          if ( ! fileName.endsWith( EXTENSION ) ) fileName += EXTENSION    ;
          outPath = directory +  File.separator + fileName ; 
        } else {
          directory = outPath.endsWith( File.separator ) ? 
                      outPath.substring(0, outPath.length() - 1 )   : 
                      outPath                                       ;
          if( outPath.endsWith( File.separator ) )
              outPath += "data" + EXTENSION                         ;
          else  outPath  +=  File.separator + "data"  +  EXTENSION  ;
        }

        /** Convert OWL TO NTriples **/
        
        if ( owl != null ) {
            
            try {
                
                LOGGER.info("OWL Conversion... " )                                                       ;
                String ontoNameWithExtension    = InOut.getfileName(owl )                                ;
                String ontoNameWithoutExtension = InOut.getFileWithoutExtension( ontoNameWithExtension ) ;
                OwlToNTriplesConverter.convert( owl , directory                            +
                                                      File.separator                       +
                                                      ontoNameWithoutExtension + ".ttl" )  ;
            } catch (FileNotFoundException ex) {
                LOGGER.error("Exception OWL Convertion", ex ) ;
            }
        }
        
        if( obda != null && ! obda.isEmpty() ) {
            
            String commandPath = null          ;
            
            try {
                
                commandPath = Utils.extractCommandQuery() ;
                
                Processor.Process( commandPath       ,
                                   obda              ,
                                   csvSeparator      ,
                                   outPath           ,
                                   limPageSize       ,
                                   fragmentFile      ,
                                   flushCount        ,
                                   parallel          ,
                                   overrideKeyInOBDA ) ;
                
                
            } catch ( Exception ex )                   {
                
                LOGGER.error( ex.getMessage() , ex )   ;
                
            } finally {
                
                try {
                    
                    if( commandPath != null )
                    InOut.rm( commandPath ) ;
                    
                } catch ( IOException ex )  {
                    LOGGER.error( ex.getMessage() , ex ) ;
                }
            }
           
        } else         {
           LOGGER.info ("                                    " ) ;
           LOGGER.info (" OBDA PATH CAN'T BE NULL OR EMPTY ! " ) ;
           LOGGER.info ("                                    " ) ;
        }
    }
    
    private static Level checkLog( String level )        {
     
        try {
             return  Level.toLevel(level.toUpperCase() ) ;
        } catch( Exception ex )  {
            LOGGER.warn(" Error : The Level "  +
                        " [" + level           +
                        "] deosn't exists."  ) ;
            LOGGER.info(" Retained LEVEL : OFF       " ) ;
            return Level.OFF                             ;
        }
    }
    
     private static int validate ( int value ) {
       
        if( value < 0 ) {
           LOGGER.error(" Values can't be Negatif !! " ) ;
           LOGGER.error( "                           " ) ;
           System.exit ( 0 )                             ;
        }
        return value ;
    }
}

