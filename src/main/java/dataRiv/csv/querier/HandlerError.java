
package dataRiv.csv.querier ;

import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import org.apache.commons.exec.LogOutputStream ;

/**
 *
 * @author ryahiaoui
 */

public class HandlerError extends LogOutputStream  {

    private static final Logger LOGGER = LogManager.getLogger( Handler.class.getName() ) ;

    @Override
    protected void processLine(String line, int logLevel) {
      
       LOGGER.info( line ) ;
   }
}