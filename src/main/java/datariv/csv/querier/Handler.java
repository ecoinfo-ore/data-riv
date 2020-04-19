
package datariv.csv.querier ;

import java.util.Map ;
import java.util.List ;
import java.util.Arrays ;
import java.util.HashMap ;
import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import org.apache.commons.exec.LogOutputStream ;

/**
 *
 * @author ryahiaoui
 */
public class Handler extends LogOutputStream {

    private int     count        =  0        ;
    private String  csvSeparator =  ";"      ;
    
    private final   Map<Integer, List<String>> queryResult  ;
    
    private static final Logger LOGGER = LogManager.getLogger( Handler.class.getName() ) ;
    
    public Handler( String delemiter ) {
       
      if( delemiter != null ) this.csvSeparator = delemiter ;
      queryResult    = new HashMap()                        ;
   }

    public void clearLogsAndInit()    {
        this.queryResult.clear()      ;
        count  = 0                    ;
    }

    public Map<Integer, List<String>>  getQueryResult()         {
        return queryResult ;
    }

    @Override
    protected void processLine(String line, int logLevel )      {
 
        String[]  queryResultLine = ( line.replace("\n", " ")   + 
                                      " " ).split( csvSeparator ) ;

        if( queryResultLine.length > 0 )  {
     
          queryResult.put( count, Arrays.asList(queryResultLine)) ;
        }
         
        count++ ;
     
        if(LOGGER.isInfoEnabled()) {
             LOGGER.debug(line )   ;
        }
    }
}