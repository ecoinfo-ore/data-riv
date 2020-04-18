
package datariv.csv.querier ;

import java.util.Map ;
import java.util.List ;
import java.util.Arrays ;
import java.util.HashMap ;
import org.apache.log4j.Logger ;
import org.apache.commons.exec.LogOutputStream ;

/**
 *
 * @author ryahiaoui
 */
public class Handler extends LogOutputStream {

    private final   Logger   logger          ;
    private final   Map<Integer, List<String>> queryResult  ;
    
    private int     count      = 0      ;
    private String  delemiter  = ";"    ;
    
    public Handler( Logger logger, boolean isError, String delemiter ) {
       
      super(isError ? -1 : 1 ) ;
      this.logger = logger     ;
      
      if( delemiter != null ) this.delemiter = delemiter ;
      
      queryResult       = new HashMap()                  ;
   }

    public void clearLogsAndInit()    {
        this.queryResult.clear()      ;
        count  = 0                    ;
    }

    public Map<Integer, List<String>>  getQueryResult()  {
        return queryResult ;
    }

    @Override
    protected void processLine(String line, int logLevel ) {
      
        String[]  queryResultLine = ( line.replace("\n", " ") +  " " ).split(delemiter) ;

        if( queryResultLine.length > 0 )  {
     
             queryResult.put( count , Arrays.asList(queryResultLine)) ;
        }
         
        count++ ;
     
       if(logLevel < 0 )       {
          logger.error( line ) ;
       }
       else {
           
          if(logger.isInfoEnabled()) {
             logger.info( line )     ;
          }
       }
   }
    
}