package com.gruter.common.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FileUtil {
  static final Log LOG = LogFactory.getLog(FileUtil.class);
  
  public static boolean delete(String path, boolean recursive) throws IOException {
    File file = new File(path);
    if(file.isDirectory()) {
      File[] srcs = file.listFiles();
      int length = (srcs != null ? srcs.length : 0) ;
      for (int i = 0; i < length; i++) {
        if(recursive) {
          if(srcs[i].isDirectory()) {
            if(!delete(srcs[i].getAbsolutePath(), recursive)) {
              return false;       
            }
          } else {
            if(!srcs[i].delete()) {
              return false;
            }
          }
        } else {
          if(!srcs[i].delete()) {
            return false;
          }
        }
      }
    }
    if(!file.delete()) {
      return false;  
    }
    
    return true;
  }  
  
  public static boolean moveFile(String oldFile, String newFile) {
    if (copyFile(oldFile, newFile)) {
      File file = new File(oldFile);
      //Windows file system 
      try {
        Thread.sleep(2 * 1000);
      } catch (InterruptedException e) {
      }
      file.delete();
      return true;
    } else {
      return false;
    }
  }

  public static boolean copyFile(String r_file, String w_file) {
    FileOutputStream foStream = null;
    String saveDirectory = null;
    File dir = null;

    try {
      int iIndex = w_file.lastIndexOf("/");

      if (iIndex > 0) {
        saveDirectory = w_file.substring(0, (w_file.lastIndexOf("/") + 1));

        dir = new File(replaceStr(saveDirectory, "//", "/"));

        if (!dir.isDirectory()) {
          dir.mkdirs();
        }

      }

      foStream = new FileOutputStream(w_file);
      return dumpFile(r_file, foStream);
    } catch (Exception ex) {
      LOG.error(ex);
      return false;
    } finally {
      try {
        if (foStream != null)
          foStream.close();
      } catch (Exception ex2) {
      }
    }
  }

  public static String replaceStr(String source, String keyStr, String toStr) {
    if (source == null)
      return null;
    int startIndex = 0;
    int curIndex = 0;
    StringBuffer result = new StringBuffer();

    while ((curIndex = source.indexOf(keyStr, startIndex)) >= 0) {
      result.append(source.substring(startIndex, curIndex)).append(toStr);
      startIndex = curIndex + keyStr.length();
    }

    if (startIndex <= source.length())
      result.append(source.substring(startIndex, source.length()));

    return result.toString();

  }  
  
  public static boolean dumpFile(String r_file, OutputStream outputstream) {
    byte abyte0[] = new byte[4096];
    boolean flag = true;
    FileInputStream fiStream = null;
    try {
      fiStream = new FileInputStream(r_file);
      int i;
      while ((i = fiStream.read(abyte0)) != -1)
        outputstream.write(abyte0, 0, i);

      fiStream.close();
    } catch (Exception ex) {
      try {
        if (fiStream != null)
          fiStream.close();
      } catch (Exception ex1) {
      }
      flag = false;
    } finally {
      try {
        if (fiStream != null)
          fiStream.close();
      } catch (Exception ex2) {
      }
    }

    return flag;
  }  
}
