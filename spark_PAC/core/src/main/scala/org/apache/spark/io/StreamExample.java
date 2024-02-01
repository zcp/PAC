package org.apache.spark.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.DataFormatException;
//import java.util.zip.Deflater;
//import java.util.zip.DeflaterInputStream;
//import java.util.zip.DeflaterOutputStream;
//import java.util.zip.Inflater;
//import java.util.zip.InflaterInputStream;

public class StreamExample {

   public static void main(String[] args) throws DataFormatException, IOException {
      String message = "Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n"
         +"Welcome to TutorialsPoint.com;\n";
      String dictionary = "Welcome";
      System.out.println("Original Message:" + message);
      System.out.println("Original Message length : " + message.length());
      byte[] input = message.getBytes("UTF-8");

      // Compress the bytes
      ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
      KaeDeflaterOutputStream outputStream = new KaeDeflaterOutputStream(arrayOutputStream);
      for(byte b: input){
         outputStream.write(b);
      }
      outputStream.close();

      //Read and decompress the data
      byte[] readBuffer = new byte[5000];
      ByteArrayInputStream arrayInputStream = 
         new ByteArrayInputStream(arrayOutputStream.toByteArray());
      KaeInflaterInputStream inputStream = new KaeInflaterInputStream(arrayInputStream);
      int read = inputStream.read(readBuffer);

      //Should hold the original (reconstructed) data
      byte[] result = Arrays.copyOf(readBuffer, read);

      // Decode the bytes into a String
      message = new String(result, "UTF-8");
      System.out.println("UnCompressed Message:" + message);
    
      System.out.println("UnCompressed Message length : " + message.length());
   }
}
