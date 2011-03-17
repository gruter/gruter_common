package com.gruter.common.metrics.system;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.gruter.common.metrics.GruterMetricsFactory;


/**
 * @author jindolk
 *
 */
public class LineKeyValueFileParser implements SystemMetricsParser {
  private static final String ALL = "*";

  private Set<String> fields = new HashSet<String>();

  private String fileName;
  
  public LineKeyValueFileParser() {
  }
  
  public void init(String fileName) throws IOException {
    this.fileName = fileName;
    String vmstatFields = GruterMetricsFactory.getFactory().getAttribute("system." + fileName + ".field");

    if(vmstatFields != null) {
      if(ALL.equals(vmstatFields)) {
        fields.add("*");
      } else {
        String[] tokens = vmstatFields.split(",");
        
        for(String eachField: tokens) {
          fields.add(eachField);
        }
      }
    }
  }

  public Map<String, Object> getMetricsValues() {
    boolean all = false;
    
    if(fields.size() == 0 || fields.contains(ALL)) {
      all = true;
    }
    
    Map<String, Object> result = new HashMap<String, Object>();
    
    BufferedReader reader = null;
    
    try {
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
      
      String line = null;
      
      while( (line = reader.readLine()) != null ) {
        if(line.trim().length() == 0) {
          continue;
        }
        int index = line.indexOf(":");
        if(index < 0) {
          index = line.indexOf(" ");
        } 
        if(index < 0) {
          GruterMetricsFactory.LOG.error(fileName + " has't key field in:" + line);
          continue;
        }

        String fieldName = line.substring(0, index);
        
        if(all || fields.contains(fieldName)) {
          result.put(fieldName, line.substring(index + 1).trim());
        }
      }
    } catch (IOException e) {
      GruterMetricsFactory.LOG.error("Can't read " + fileName + ": " + e.getMessage(), e);
    } finally {
      if(reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
        }
      }
    }
    
    return result;
  }

}
