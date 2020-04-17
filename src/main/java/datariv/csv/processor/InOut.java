
 package datariv.csv.processor ;

 import java.io.File ;
 import java.util.List ;
 import java.nio.file.Path ;
 import java.io.IOException ;
 import java.nio.file.Files ;
 import java.nio.file.Paths ;
import org.apache.commons.io.FileUtils ;
 import java.nio.file.StandardOpenOption ;
 import java.nio.charset.StandardCharsets ;

 /**
 *
 * @author ryahiaoui
 */

 public class InOut {

    public static void rm(String path ) throws IOException {
      System.out.println(" Remove Path : " + path ) ;
      if(new File(path).isDirectory()) {
        FileUtils.deleteDirectory(new File(path)) ;
      }
      else {
        new File(path).delete() ;
      }
    }

    public static void writeTextFile(  String fileName, List<String> strLines ) throws IOException {
        
      Path path = Paths.get(fileName)         ;
      Files.write(  path                      , 
                    strLines                  ,
                    StandardCharsets.UTF_8    ,
                    StandardOpenOption.CREATE , 
                    StandardOpenOption.APPEND ) ;
    }
    
    public static String getFolder(String outputFile ) {
      Path path = Paths.get(outputFile)  ;
      return path.getParent().toString() ;
    }

    public static String getfileName(String outputFile) {
      Path path = Paths.get(outputFile)    ;
      return path.getFileName().toString() ;
    }    
      
    public static String getFileExtension( String  fileName ) {      
      if( fileName.lastIndexOf(".") != -1 && 
          fileName.lastIndexOf(".") != 0   )
      return fileName.substring( fileName.lastIndexOf( ".") ) ;
      else return "" ;
    }
    
    public static String getFileWithoutExtension( String fileName ) {      
      return fileName.replaceFirst("[.][^.]+$", "") ;
    }
    
 }