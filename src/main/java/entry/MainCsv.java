
package entry ;

import java.io.IOException ;
import datariv.csv.processor.InOut ;
import datariv.csv.runnerquery.Utils ;
import datariv.csv.processor.Processor ;
import datariv.owl.ntriple.OwlToNTripleConverter ;

/**
 *
 * @author ryahiaoui
 */

public class MainCsv {
    
    public static void main(String[] args) throws IOException, Exception {
        
        final int LIMIT_BATCH_SIZE = 100_000 ; // LIMIT
        final int FRAGMENT_FILE    = 10_000  ; // TOTAL LINES IN EACH FILE ( CLOSEST VALUE )
        final int FLUSH_COUNT      = 500_000 ; // DATA IN MEMORY BEFORE FLUSH
        
        final String obdaPath      = "/home/ryahiaoui/Téléchargements/JAXY_COBY/COBY-2828/coby_standard_bin/pipeline/SI/FORET/output/01_obda/5_mapping_CSV_LatentHeat.obda" ;
          
        final String outPathData   =  "/home/ryahiaoui/Téléchargements/TO_DELL/3/Ontop-materializer/v3/out/data.ttl" ;
        
        final String csvDelemiter  = ";"  ;
        
        boolean parallel           = true ;
        
        String commandPath = Utils.extractCommandQuery() ;
        
        Processor.Process( commandPath      ,
                           obdaPath         ,
                           csvDelemiter     ,
                           outPathData      ,
                           LIMIT_BATCH_SIZE ,
                           FRAGMENT_FILE    , 
                           FLUSH_COUNT      ,
                           parallel      )  ;
        
        
       InOut.rm( commandPath )              ;
       
       
       /** Convert OWL TO NTriples **/
       
       String owlPath = "/home/ryahiaoui/.local/share/Trash/files/ontop-matarializer/src/main/resources/mapping/ontology.owl" ;
       String outPath = "/home/ryahiaoui/Téléchargements/TO_DELL/3/Ontop-materializer/v3/out/ontology.ttl"                    ;
       
       OwlToNTripleConverter.convert( owlPath, outPath ) ;
    }
    
}
