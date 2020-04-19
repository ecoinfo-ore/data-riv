
package datariv.core ;

import java.util.Set ;
import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import it.unibz.inf.ontop.model.term.Variable ;

/**
 *
 * @author ryahiaoui
 */

public class Mapping                   {
    
     
    private final String id            ;
    private final String query         ;
    private final String tripleMapping ;

    private final Set<Variable> variablesMapping ;

    private static final Logger LOGGER = LogManager.getLogger( Mapping.class.getName() ) ;
    
    public Mapping( String id                       , 
                    String query                    , 
                    String tripleMapping            ,
                    Set<Variable> variablesMapping  ) {
        
        this.id               = id                  ;
        this.query            = cleanQuery( query ) ;
        this.tripleMapping    = tripleMapping       ;
        this.variablesMapping = variablesMapping    ;
    }

    public String getId()    {
        return id  ;
    }

    public String getQuery() {
        return query ;
    }

    public String getTripleMapping() {
        return tripleMapping ;
    }

    public Set<Variable> getVariablesMapping() {
        return variablesMapping ;
    }

    private String cleanQuery( String query  ) {
        
       query = query.replaceAll(" +", " ")
                    .replaceAll("\n", " ")
                    .trim() ;
       
      return query.endsWith(";")         ? 
        query.substring( 0, query.trim()
                                 .length()-1 )
                                 .trim() :
        query ;
    }

}

