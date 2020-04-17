
package datariv.csv.runnerquery ;

import java.io.File ;
import java.util.Map ;
import java.util.List ;
import java.util.Arrays ;
import java.io.IOException ;
import java.io.InputStream ;
import java.util.ArrayList ;
import java.io.OutputStream ;
import java.util.stream.Stream ;
import java.util.regex.Matcher ;
import java.util.regex.Pattern ;
import java.io.FileOutputStream ;
import org.apache.commons.exec.CommandLine ;
import org.apache.commons.lang3.SystemUtils ;
import org.apache.commons.exec.DefaultExecutor ;

/**
 *
 * @author ryahiaoui
 */
public class Utils {
    
    final static Pattern REGEX   = Pattern.compile("\'[^']*'|\"[^\"]*\"|( )") ;
    final static String  SPLITER = "@#*_SplitHere_*#@"                        ;

    public static DefaultExecutor getExecutor()        {
        
      DefaultExecutor executor = new DefaultExecutor() ;
      executor.setExitValue(0)                         ;     
      return executor ;
    }
     
    public static CommandLine buildCommandLine( String command, String argCmd )     {
        
       System.out.println(" \n Running Command : " + command + " " +  argCmd + "\n" ) ;
      
      /**  Splitting on SPACE  outside DOUBLE QUOTES  **/
      String[] cmdArgs     = cleanAndParseCommand( argCmd ) ; 
        
      CommandLine cmdLine  = CommandLine.parse(command)     ;

      Stream.of( cmdArgs )
            .forEach( arg ->  {
                                if( ! arg.trim().isEmpty())
                                cmdLine.addArgument( arg.trim() , 
                                                     false )    ;
      }) ;
      
      return cmdLine ;
    }
    
    public static String applyLimitOffsetParams ( String query , int LIMIT ) {
      
        String templateQuery = cleanQuery( query )  ;
        
        if( LIMIT >= 0 ) {
            
            Pattern p = Pattern.compile( " LIMIT +.?\\d+"           , 
                                         Pattern.CASE_INSENSITIVE ) ;
            Matcher m = p.matcher( templateQuery )  ;

            String limit = "" , offset = ""         ;

            if(m.find()) {
                 limit = m.group() ;
            }

            p = Pattern.compile( " OFFSET +.?\\d+"          , 
                                 Pattern.CASE_INSENSITIVE ) ;
            m = p.matcher( templateQuery ) ;

            if(m.find()) {
                 offset = m.group() ;
            }
  
            if( limit.isEmpty() )  {
               templateQuery    += " LIMIT " + LIMIT + " " ;
            }
            else {
               templateQuery =  templateQuery.replace( limit, " LIMIT 0 " )  ;
            }
            
            if( offset.isEmpty() ) {
                templateQuery += " OFFSET 0 " ;
            }
            else {
                templateQuery = templateQuery.replace( offset, " " )  + " OFFSET 0 " ;
            }
            
            templateQuery = templateQuery.replaceAll(" +", " " )
                                         .trim() + " "         ;
      }
        
      return templateQuery ;
    }
     
    public static <K, V> Stream<K> getKeyByValue(Map<K, V> map, V value) {
    
         return map.entrySet()
                   .stream()
                   .filter(entry -> value.equals(entry.getValue()))
                   .map(Map.Entry::getKey) ;
   
    }
     
    public static String extractCommandQuery() throws IOException {
       
       System.out.print("Extract Q-Data Command... : " ) ;
  
       String commandPath = "q-linux"      ;
       
       if( SystemUtils.IS_OS_WINDOWS )     {
           commandPath = "q-windows.exe"   ;
       }
       
       System.out.println( commandPath )   ;
       
       extractExec( "q", commandPath , "." )  ;
      
       if( SystemUtils.IS_OS_LINUX )          {

          commandPath = "./" + commandPath    ;
          Runner.runCmd( "chmod",
                         "777 " + commandPath ) ;
       }
       
      return commandPath  ;

    }
   
    private static String[] cleanAndParseCommand(String command ) {

        Matcher      m = REGEX.matcher(  command.trim() ) ;
        StringBuffer b = new StringBuffer()               ;

        while (m.find()) {
            if (m.group(1) != null) {
                m.appendReplacement(b, SPLITER ) ;
            } else {
                m.appendReplacement(b, m.group(0));
            }
        }
        
        m.appendTail(b);
        String replaced = b.toString()                           ;
        String[] splits = replaced.split(Pattern.quote(SPLITER)) ;
        List<String> l = new ArrayList<>()                       ;

        Arrays.stream(splits).forEach( arg -> {
            if (!arg.trim().isEmpty()) {
                l.add( removeDoubleQuotes(arg.trim())) ;
            }
        });

        return Arrays.copyOf(l.toArray(), l.size(), String[].class ) ;
    }
   
    private static String removeDoubleQuotes(String arg ) {
        
       if( arg.startsWith("\"") && arg.endsWith("\""))    {
       return arg.substring(1, arg.length() -1 )          ;
       }
       return arg ;
    }
    
    private static String cleanQuery( String query ) {
      return query.endsWith(";")                    ? 
             query.replaceAll(" +" , " ")
                  .replaceAll("\t+", " ")
                  .substring( 0, query.length()-1 )
                  .trim()  :
             query.replaceAll(" +", " ") 
                   .replaceAll("\t+", " ").trim()   ;
    }
    
    private static void extractExec( String path , String prg , String dest ) throws IOException {
        
        OutputStream os ;
        
        try (InputStream is = Utils.class.getClassLoader()
                                         .getResource( path + "/" + prg  )
                                         .openStream())                  {
            os = new FileOutputStream( dest + File.separator + prg )     ;
            byte[] b = new byte[2048]             ;
            int length                            ;
            while (( length = is.read(b)) != -1 ) {
                  os.write(b, 0, length)          ;
            }
        }
        os.close() ;
    }

}