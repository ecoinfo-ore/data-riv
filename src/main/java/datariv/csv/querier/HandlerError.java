
package datariv.csv.querier ;

import org.apache.log4j.Logger ;
import org.apache.commons.exec.LogOutputStream ;

/**
 *
 * @author ryahiaoui
 */
public class HandlerError extends LogOutputStream         {

    private final   Logger   logger ;
    
    public HandlerError( Logger logger, boolean isError ) {
       
      super(isError ? -1 : 1 )      ;
      this.logger = logger          ;
   }

    @Override
    protected void processLine(String line, int logLevel) {
      
       if(logLevel < 0 )     {
          logger.error(line) ;
       }
       // Normal output stream
       else {
          if(logger.isInfoEnabled()) {
             logger.info( line )     ;
          }
       }
   }
}