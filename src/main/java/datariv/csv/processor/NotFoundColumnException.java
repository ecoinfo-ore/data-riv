
package datariv.csv.processor ;

import java.util.Collection ;

/**
 *
 * @author ryahiaoui
 */

public class NotFoundColumnException extends RuntimeException {

    public NotFoundColumnException( String message ) {
        super( message ) ;
    }

    public NotFoundColumnException (  String mappingID , String variable , Collection<String> columns ) {
      
        super ( "\n\n Exception occurred At Mapping ID { " + mappingID  + " } // The Variable [ " + 
                variable + " ]  Doesn't match any Column \n Columns Result  : "  + columns       + " \n" ) ;
    }
}

