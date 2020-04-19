
package datariv.csv.processor ;

import java.util.Set ;
import java.util.List ;
import datariv.core.Mapping ;
import java.util.regex.Matcher ;
import java.util.regex.Pattern ;
import org.apache.logging.log4j.Logger ;
import org.apache.logging.log4j.LogManager ;
import static java.text.MessageFormat.format ;
import it.unibz.inf.ontop.model.term.Variable ;

/**
 *
 * @author ryahiaoui
 */
public class MappingAdapter             {
    
    private final Mapping mapping       ;
    
    private Integer  limit  =  null     ;
    private String   instanceQuery      ;
    private String   templateQuery      ;
    
    private          int   OFFSET     =  0  ;
    public static    int   PAGE_SIZE  =  0  ;
    
    private boolean  alreadyExistsLimitOffset = false ;
   
    private final int TOTAL_LINE_PER_TRIPLE_MAPPING   ;
    
    private static final Logger LOGGER = LogManager.getLogger( MappingAdapter.class.getName() ) ;
    
    
    public MappingAdapter( Mapping mapping ) {
        
      this.mapping                  = mapping                 ;
      this.templateQuery            = this.mapping.getQuery() ;
      TOTAL_LINE_PER_TRIPLE_MAPPING = countLines( this.mapping.getTripleMapping() ) ;
    }


    public String getId()           {
        return this.mapping.getId() ;
    }

    public String getQuery()           {
        return this.mapping.getQuery() ;
    }

    public String getTripleMapping()           {
        return this.mapping.getTripleMapping() ;
    }

    public String applyOffset( Integer OFFSET )               {
        
        this.OFFSET   = OFFSET                                ;
        instanceQuery = templateQuery.replaceAll("'","<>")    ;        
        instanceQuery = format( instanceQuery , 
                                OFFSET.toString() )
                                      .replaceAll("<>", "'")  ;
        
        LOGGER.debug ( " OFFSET : "     + this.OFFSET      )  ;
        LOGGER.debug ( " InstanceQuery" + instanceQuery    )  ;
        
        return instanceQuery ;

    }
    
    public void initLimitOffsetAndOverrideParams ( List<String> columns , int LIMIT ) {
      
        if( LIMIT >= 0 ) {
            
            instanceQuery = this.mapping.getQuery()                 ;

            Pattern p    = Pattern.compile( " LIMIT +.?\\d+"        , 
                                         Pattern.CASE_INSENSITIVE ) ;
            
            Matcher m    = p.matcher( this.mapping.getQuery() )     ;

            String _limit = "" , offset = ""   ;

            if(m.find()) {
                 _limit = m.group()            ;
            }

            p = Pattern.compile( " OFFSET +.?\\d+"          , 
                                 Pattern.CASE_INSENSITIVE ) ;
            
            m = p.matcher( this.mapping.getQuery() )        ;

            if(m.find()) { offset = m.group()  ;   }
             
            if( ! this.mapping.getQuery().toLowerCase()
                              .contains( "order by " ) )    {
              
              templateQuery += " ORDER BY " + 
                               String.join(", " , columns ) +
                                " " ;
            }
                
            if( _limit.isEmpty() ) {
                
               templateQuery += " LIMIT " + LIMIT + " "     ;
               this.limit     =   LIMIT                     ;
               
            }
            else {
                
               alreadyExistsLimitOffset = true                     ;
               
               this.limit = Integer.parseInt(  _limit.toLowerCase( )
                                   .replace(" limit ", "" )   )    ;
               
               templateQuery =  templateQuery.replace( _limit, " " )  
                                + _limit + " "                     ;
            }
           
           /* OFFSET MUST BE AT THE END  */
          
            if( offset.isEmpty() )                              {
                templateQuery  += " OFFSET {0} "                ;
            }
            else {
                alreadyExistsLimitOffset = true                 ;
                templateQuery  = templateQuery.replace( offset  , 
                                                         " "    ) 
                                  + offset                      ;
            }
            
            LOGGER.debug( " TemplateQuery : " + templateQuery ) ;
            
            templateQuery = templateQuery.replaceAll(" +" , " " )
                                         .trim()     + " "      ;
      }
    }
    
    public Integer getLimit() {
        return limit          ;
    }
    
    public int getOFFSET()    {
        return OFFSET         ;
    }

    public boolean isAlreadyExistsLimitOffset()   {
        return alreadyExistsLimitOffset  ;
    }

    public Set<Variable> getVariablesMapping()    {
        return this.mapping.getVariablesMapping() ;
    }


    private int countLines(String tripleMapping ) {
        
     return tripleMapping.split (
            System.getProperty( "line.separator") )
           .length ;
    }

    public int getTotalLinesPerTripleMapping()    {
        return TOTAL_LINE_PER_TRIPLE_MAPPING      ;
    }
}
