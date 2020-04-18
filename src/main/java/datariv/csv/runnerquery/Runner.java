
package datariv.csv.runnerquery ;

import java.util.Map ;
import java.util.List ;
import java.util.HashMap ;
import java.util.ArrayList ;
import java.io.IOException ;
import org.apache.log4j.Logger ;
import java.util.stream.Stream ;
import java.io.ByteArrayOutputStream ;
import org.apache.commons.exec.CommandLine ;
import org.apache.commons.exec.DefaultExecutor ;
import org.apache.commons.exec.PumpStreamHandler ;
import static datariv.csv.runnerquery.Utils.getExecutor ;
import org.apache.commons.exec.DefaultExecuteResultHandler ;
import static datariv.csv.runnerquery.Utils.buildCommandLine ;
import static datariv.csv.runnerquery.Utils.applyLimitOffsetParams ;

/**
 *
 * @author ryahiaoui
 */

public class Runner {
    
    final  static Logger           LOGGER =  Logger.getLogger(Runner.class) ;
   
    private final DefaultExecutor  executor             ;
   
    private final Handler          resultQueryHandler   ;
    
    private final String           commandQuery         ;
    
    int                            columnCounter    = 0 ;
    
    private final String           originalQuery        ;
   
    private final String           csvDelemiter         ;
    
    private final Map<Integer, String > columnNames     ;

    
    public Runner( String commandQuery, String originalQuery , String csvDelemiter ) throws IOException {
        
        this.executor      = getExecutor()   ;
        
        this.commandQuery  = commandQuery    ;

        this.originalQuery = originalQuery   ;
        
        this.csvDelemiter  = csvDelemiter    ;
        
        this.columnNames   = extractColumnsName( commandQuery  ,
                                                 originalQuery , 
                                                 csvDelemiter  ) ;
        
        this.resultQueryHandler = configHandlerForQueryExecutor( executor     , 
                                                                 csvDelemiter ) ;
    }
    
    public Map<Integer, List<String>> runCommandQuery( String argCommandQuery , boolean async  ) throws Exception {
     
      resultQueryHandler.clearLogsAndInit()   ;
        
      String cmdQuery = " -H --delimiter \"" + csvDelemiter + "\"  \""  +
                        argCommandQuery    + "\""                       ;
       
      CommandLine cmdLine = buildCommandLine( commandQuery , cmdQuery ) ;
      
      if( async ) {
          
          DefaultExecuteResultHandler resultHandler = 
                            
                            new DefaultExecuteResultHandler()      ;

          executor.execute(cmdLine, resultHandler )                ;

          resultHandler.waitFor( 1000 )                            ;

          if( resultHandler.hasResult() )                          {

              if( resultHandler.getException() != null )           {
                  
                  System.out.println( "\n *** ERROR  *** "         + 
                                      resultHandler.getException()
                                                   .getMessage())  ;
                  
              }
              
              System.out.println(" Check LOG FILES \n" )           ;
          }
          
          return null ;
          
      } else {
         
            executor.execute( cmdLine )                ;
            
            return resultQueryHandler.getQueryResult() ;
      }
    }

    private static Handler configHandlerForQueryExecutor( DefaultExecutor executor       , 
                                                          String          csvDelemiter ) {
        
       Handler            outReshANDLER =   new Handler( LOGGER, true , csvDelemiter )   ;
       HandlerError       err           =   new HandlerError(LOGGER, true         )      ;
      
       PumpStreamHandler  pp            =   new PumpStreamHandler( outReshANDLER         , 
                                                                   err                   ,
                                                                   System.in   )         ;
       executor.setStreamHandler ( pp ) ;
       return outReshANDLER             ;
    }
    
    private Map<Integer, String > extractColumnsName( String commandQuery  , 
                                                      String originalQuery ,
                                                      String csvDelemiter  ) throws IOException {
        
        String query = buildQueryColumnsNames( originalQuery, csvDelemiter )  ;
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()      ;
        
        CommandLine commandline = buildCommandLine( commandQuery , query )    ;

        DefaultExecutor exec            = new DefaultExecutor()               ;
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream) ;
        
        exec.setStreamHandler(streamHandler) ;
        
        exec.execute(commandline)            ;
        
        String   line                  = outputStream.toString()
                                                     .replace("\n", "" ) ;
        
        String[] queryResultLine       = line.split(csvDelemiter )       ;
      
        Map<Integer, String > colNames = new HashMap<>()                 ;
        
        Stream.of ( queryResultLine).forEach ( column ->  {
          colNames.put ( columnCounter ++ ,    column  )  ;
        } ) ;
        
        return colNames ;
    }

    private String buildQueryColumnsNames(String query , String csvDelemiter )    {
        
        String QueryWithLimit_0_Offset_0 = applyLimitOffsetParams ( query , 0 )   ;
        
        return " -O -H --delimiter \""    + csvDelemiter + "\"  \"" +
               QueryWithLimit_0_Offset_0  + "\""                    ;
    }
    
    public static void runCmd( String commandQuery , 
                               String commandArgs  ) throws IOException           {
        
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream()          ;
        
        CommandLine commandline = buildCommandLine( commandQuery , commandArgs )  ;
        DefaultExecutor exec    = new DefaultExecutor()                           ;
        
        PumpStreamHandler streamHandler = new PumpStreamHandler(outputStream)     ;
        
        exec.setStreamHandler(streamHandler)          ;
        exec.execute(commandline)                     ;
        
        String line = outputStream.toString()         ;
        if( line != null && ! line.isEmpty()  )
        System.out.println( "Cmd Output : " + line )  ;
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
