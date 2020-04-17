
package datariv.owl.ntriple ;

/**
 *
 * @author ryahiaoui
 */

import java.io.File ;
import java.io.FileOutputStream ;
import java.io.FileNotFoundException ;
import org.apache.jena.rdf.model.Model ;
import org.apache.jena.ontology.OntModelSpec ;
import org.apache.jena.rdf.model.ModelFactory ;

public class OwlToNTripleConverter {

    public static void convert( String owlPath, String outPathFile ) throws FileNotFoundException {
        
      OntModelSpec spec = OntModelSpec.OWL_DL_MEM_RDFS_INF       ;
 
      Model model       = ModelFactory.createOntologyModel(spec) ;

      model.read( owlPath ) ;
      
      File file            = new File( outPathFile )      ;
      FileOutputStream fop = new FileOutputStream( file ) ;

      model.write( fop , "NT") ;
    
    }
 
}
