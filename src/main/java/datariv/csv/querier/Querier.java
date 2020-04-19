
package datariv.csv.querier ;

import java.util.Map ;
import java.util.List ;
import java.util.HashMap ;
import java.util.ArrayList ;
import java.io.IOException ;
import java.util.stream.Stream ;
import java.io.ByteArrayOutputStream ;
import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import org.apache.commons.exec.CommandLine ;
import org.apache.commons.exec.DefaultExecutor ;
import org.apache.commons.exec.PumpStreamHandler ;
import static datariv.csv.querier.Utils.getExecutor ;
import static datariv.csv.querier.Utils.buildCommandLine ;
import org.apache.commons.exec.DefaultExecuteResultHandler ;
import static datariv.csv.querier.Utils.applyLimitOffsetParams ;

/**
 *
 * @author ryahiaoui
 */

public class Querier   {
    
    final  static Logger                LOGGER =  LogManager.getLogger( Querier.class.getName()) ;
   
    private final DefaultExecutor       executor             ;
   
    private final Handler               resultQueryHandler   ;
    
    private final String                commandQuery         ;
    
    int                                 columnCounter    = 0 ;

    private final String                originalQuery        ;
   
    private final String                csvSeparator         ;
    
    private final Map<Integer, String > columnNames          ;

    
    public Querier( String commandQuery, String originalQuery , String csvSeparator ) throws IOException {
        
        this.executor      = getExecutor()   ;
        
        this.commandQuery  = commandQuery    ;

        this.originalQuery = originalQuery   ;
        
        this.csvSeparator  = csvSeparator    ;
        
        this.columnNames   = extractColumnsName( commandQuery  ,
                                                 originalQuery , 
                                                 csvSeparator  ) ;
        
        this.resultQueryHandler = configHandlerForQueryExecutor( executor     , 
                                                                 csvSeparator ) ;
    }
    
    public Map<Integer, List<String>> runCommandQuery( String argCommandQuery , boolean async  ) throws Exception {
     
      resultQueryHandler.clearLogsAndInit()   ;
        
      String cmdQuery = " -H --delimiter \"" + csvSeparator + "\"  \""  +
                        argCommandQuery    + "\""                       ;
       
      CommandLine cmdLine = buildCommandLine( commandQuery , cmdQuery ) ;
      
      if( async ) {
          
          DefaultExecuteResultHandler resultHandler = 
                            
                            new DefaultExecuteResultHandler()      ;

          executor.execute(cmdLine, resultHandler )                ;

          resultHandler.waitFor( 1000 )                            ;

          if( resultHandler.hasResult() )                          {

              if( resultHandler.getException() != null )           {
                  
                  LOGGER.error( " *** ERROR  *** "           + 
                                resultHandler.getException()
                                             .getMessage())  ;
              }
          }
          
          return null ;
          
      } else {
         
            executor.execute( cmdLine )                ;
            
            return resultQueryHandler.getQueryResult() ;
      }
    }

    private static Handler configHandlerForQueryExecutor( DefaultExecutor executor       , 
                                                          String          csvDelemiter ) {
        
       Handler            outReshANDLER =   new Handler     ( csvDelemiter )   ;
       HandlerError       err           =   new HandlerError( )                ;
      
       PumpStreamHandler  pp            =   new PumpStreamHandler( outReshANDLER         , 
                                                                   err                   ,
                                                                   System.in   )         ;
       executor.setStreamHandler ( pp ) ;
       return outReshANDLER             ;
    }
    
    private Map<Integer, String > extractColumnsName( String commandQuery  , 
                                                      String originalQuery ,
                                                      String csvSeparator  ) throws IOException {
        
        String query = buildQueryColumnsNames( originalQuery, csvSeparator )  ;
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()      ;
        
        CommandLine commandline = buildCommandLine( commandQuery , query )    ;

        DefaultExecutor exec            = new DefaultExecutor()               ;
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream) ;
        
        exec.setStreamHandler(streamHandler) ;
        
        exec.execute( commandline )          ;
        
        String   line                  = outputStream.toString()
                                                     .replace("\n", "" ) ;
        
        String[] queryResultLine       = line.split(csvSeparator )       ;
      
        Map<Integer, String > colNames = new HashMap<>()                 ;
        
        Stream.of ( queryResultLine).forEach ( column ->  {
          colNames.put ( columnCounter ++ ,    column  )  ;
        } ) ;
        
        return colNames ;
    }

    private String buildQueryColumnsNames(String query , String csvDelemiter )   {
        
        String QueryWithLimit_0_Offset_0 = applyLimitOffsetParams ( query , 0 )  ;
        
        return " -O -H --delimiter \""    + csvDelemiter + "\"  \"" +
               QueryWithLimit_0_Offset_0  + "\""                    ;
    }
    
    public static void runCmd( String commandQuery , 
                               String commandArgs  ) throws IOException          {
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()         ;
        
        CommandLine commandline = buildCommandLine( commandQuery , commandArgs ) ;
        DefaultExecutor exec    = new DefaultExecutor()                          ;
        
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream)    ;
        
        exec.setStreamHandler(streamHandler)          ;
        exec.execute(commandline)                     ;
        
        String line = outputStream.toString()         ;
        if( line != null && !  line.isEmpty()  )
        LOGGER.info( "Cmd Output : " + line )  ;
    }
    
    public Map<Integer, String> getColumnsName()  {
       return this.columnNames  ;
    }
    
    public List<String> getColumnsNameAsList()    {
       return new ArrayList ( 
                     getColumnsName().values() )  ;
    }
    
    public void clearResultQuery() {
      resultQueryHandler.getQueryResult().clear() ;
    }
    
}
