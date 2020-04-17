package datariv.core ;

import java.util.Set ;
import java.util.List ;
import java.util.Optional ;
import java.util.ArrayList ;
import java.util.stream.Collectors ;
import it.unibz.inf.ontop.model.term.Variable ;
import it.unibz.inf.ontop.model.type.TermType ;
import com.google.common.collect.ImmutableList ;
import it.unibz.inf.ontop.model.atom.TargetAtom ;
import it.unibz.inf.ontop.model.term.ImmutableTerm ;
import it.unibz.inf.ontop.spec.mapping.pp.SQLPPMapping ;
import it.unibz.inf.ontop.spec.mapping.pp.SQLPPTriplesMap ;
import it.unibz.inf.ontop.injection.OntopSQLOWLAPIConfiguration ;



/**
 *
 * @author ryahiaoui
 */
public class ObdaManager {

    static final String CSV_KEY = "csv-directory" ;
     
    public static List<Mapping> loadOBDA(String obdaFile) throws Exception     {
       
        OntopSQLOWLAPIConfiguration build = OntopSQLOWLAPIConfiguration.defaultBuilder()
                                                                       .nativeOntopMappingFile(obdaFile)
                                                                       .jdbcUrl("jdbc:postgresql:" )
                                                                       .jdbcDriver("org.postgresql.Driver")
                                                                       .jdbcUser("--")
                                                                       .jdbcPassword("--")
                                                                       .enableTestMode()
                                                                       .build();
        String csvDirectory = build.loadPPMapping()
                                   .get()
                                   .getMetadata()
                                   .getPrefixManager()
                                   .getPrefixMap()
                                   .asMultimap()
                                   .asMap()
                                   .get(CSV_KEY + ":" )
                                   .stream()
                                   .findFirst()
                                   .orElse(null) ;
        
        Optional<SQLPPMapping> loadPPMapping =
                
                OntopSQLOWLAPIConfiguration.defaultBuilder()
                                           .nativeOntopMappingFile(obdaFile)
                                           .jdbcUrl("jdbc:postgresql:" )
                                           .jdbcUser("--")
                                           .jdbcPassword("--")
                                           .enableTestMode()
                                           .build().loadPPMapping()                   ; 

        List<Mapping> mappings = new ArrayList<>()                                    ; 
        
        for ( int i = 0 ; i <  loadPPMapping.get().getTripleMaps().size(); i++ )      {
           
            SQLPPTriplesMap sqlTripleMap = loadPPMapping.get().getTripleMaps().get(i) ;
        
            String          idMap        =  sqlTripleMap.getId()                      ;
            
            String          query        =  sqlTripleMap.getSourceQuery()
                                                        .getSQLQuery()
                                                        .replace (CSV_KEY       , 
                                                                   csvDirectory )     ;
            
            List<TargetAtom> targetAtoms = sqlTripleMap.getTargetAtoms().asList()     ;
            
            String tripleMapping = targetAtoms.stream()
                                              .map((targetAtom) -> targetAtom.getSubstitutedTerms() )
                                              .map((term) -> getSubject  ( term ) + " "  + 
                                                             getPredicate( term ) + " "  +
                                                             getObject   ( term ) + " .\n" )
                                              .reduce(String::concat ).get()               ;
            
            /** Remove Last New Line */
            tripleMapping =  tripleMapping.substring( 0, tripleMapping.length()-1 )        ;
           
            Set<Variable> variableMapping = targetAtoms.stream()
                                                       .map((targetAtom) -> targetAtom.getSubstitutedTerms() )
                                                       .map((term) ->  getVariablesMapping(term)  )
                                                       .flatMap( List::stream )
                                                       .collect(Collectors.toSet())       ;
         
            mappings.add( new Mapping( idMap, query , tripleMapping , variableMapping ) ) ;
        }
        
        return mappings ;        

    }

    private static String getSubject(ImmutableList<ImmutableTerm> immutableTerm )       {
        
       ImmutableTerm term    =  immutableTerm.get( 0 )                                  ;
        
       String        subject =  term.simplify().toString()                              ;
       
       if ( ! subject.startsWith("<") && ! subject.endsWith(">") )                      {
             subject = subject.split("\\(")[1].split("\\(")[0]                          ;
       }

       List<Variable> variables = term.getVariableStream().collect(Collectors.toList()) ;
       
       for ( Variable variable : variables ) {
           subject = subject.replaceFirst( "\\{\\}",
                                          "{"      + variable.getName().trim() + "}")  ; 
       }
        
       return "<" + subject +  ">" ;
    }

    private static String getPredicate(ImmutableList<ImmutableTerm> immutableTerm )    {
     
        ImmutableTerm term = immutableTerm.get( 1 )                                    ;
        
        String predicate = term.simplify().toString()                                  ;
       
        List<Variable> variables = term.getVariableStream()
                                       .collect( Collectors.toList() )                 ;
       
        for ( Variable variable :  variables ) {
           predicate = predicate.replaceFirst( "\\{\\}",
                                               "{" + variable.getName().trim() + "}" ) ; 
        }
        
        return predicate ;
    }

    private static String getObject (ImmutableList<ImmutableTerm> immutableTerm ) {
        
        ImmutableTerm  term      = immutableTerm.get( 2 )                         ;
        
        String         object    = term.simplify().toString()                     ;
        
        TermType       termType  = term.inferType().get().getTermType().get()     ;
        
        List<Variable> variables = term.getVariableStream()
                                       .collect(Collectors.toList()) ;
       
        if( termType.toString().equalsIgnoreCase("IRI") ||
            termType.toString().equalsIgnoreCase("URI"  ))          {
            
            if( ! object.startsWith("<") && ! object.endsWith(">")) {
                object = "<" + object.split("\\(")[1].split("\\(")[0] + ">"       ;
            }
            
            for ( Variable variable :  variables ) {
               object = object.replaceFirst( "\\{\\}" , 
                                             "{" + variable.getName().trim() + "}") ; 
            }
            
        } else {
            
             // There's ONLY ZERO OR ONE VARIABLE
            if( variables.size() == 1 ) {
                
                object = "\"{"   + variables.get(0).getName() +
                         "}\"^^" + termType.toString()        ;
            } 
            else if ( ! variables.isEmpty() ) { // SIZE >= 2
                
                throw new IllegalArgumentException( " OBJECT : [ " + object        + 
                                                    "] Must Have Only Zero Or "    +
                                                    "One Variable ! \n "           +
                                                    " VARIABLES = " + variables  ) ;
            }
        }
       
       return object ;
    }
    
     private static List<Variable> getVariablesMapping(ImmutableList<ImmutableTerm> immutableTerm )  {
     
        List<Variable> variableMapping = new ArrayList<>() ;
        // SUBJECT   : 0
        variableMapping.addAll( immutableTerm.get(0).getVariableStream().collect(Collectors.toList() ) ) ;
        // PREDICATE : 1
        variableMapping.addAll( immutableTerm.get(1).getVariableStream().collect(Collectors.toList() ) ) ;
        // OBJECT    : 2
        variableMapping.addAll( immutableTerm.get(2).getVariableStream().collect(Collectors.toList() ) ) ;
        
        return variableMapping ;
       
     }
       
}

