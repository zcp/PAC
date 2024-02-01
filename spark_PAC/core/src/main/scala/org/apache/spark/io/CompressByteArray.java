/*
 Compress Byte Array Using Deflater Example
 This Java example shows how to compress the byte array using
 Java Deflater class. 
 
 Deflater class provides support for general purpose compression
 using ZLIB compression library. 
*/
 
package org.apache.spark.io;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
//import java.util.zip.Deflater;
//import java.util.zip.Inflater;
public class CompressByteArray {
 
 public static void main(String args[]){
 String str = "aaaaaaaaaa bbbbbbbb cccccc";
 //String str = "Compress byte array using Deflater example";
 
 //get bytes
 byte[] bytes = str.getBytes();
 
 /*
 * To create an object of Deflater, use
 * 
 * Deflater()
 * Constructor of Deflater class.
 * 
 * This will create a new compressor with
 * default compression level. 
 */
 
 KaeDeflater deflater = new KaeDeflater();
 
 /*
   * Set the input of compressor using,
   * 
   * setInput(byte[] b)
   * method of Deflater class.
   */
   
   deflater.setInput(bytes);
   
   /*
    * We are done with the input, so say finish using
    * 
    * void finish()
    * method of Deflater class.
    * 
    * It ends the compression with the current contents of
    * the input.
    */
    
    deflater.finish();
    
    /*
     * At this point, we are done with the input.
     * Now we will have to create another byte array which can
     * hold the compressed bytes.
    */
    
    ByteArrayOutputStream bos = new ByteArrayOutputStream(bytes.length);
    
    byte[] buffer = new byte[1024];
    
    /*
     * Use
     * 
     * boolean finished()
     * method of Deflater class to determine whether
     * end of compressed data output stream reached.
     *
     */
    while(!deflater.finished())
    {
    /*
    * use
    * int deflate(byte[] buffer) 
    * method to fill the buffer with the compressed data.
    * 
    * This method returns actual number of bytes compressed.
    */
    
    int bytesCompressed = deflater.deflate(buffer);
    bos.write(buffer,0,bytesCompressed);
    }
    
    try
    {
    //close the output stream
    bos.close();
    }
    catch(IOException ioe)
    {
    System.out.println("Error while closing the stream : " + ioe);
    }
    
    //get the compressed byte array from output stream
    byte[] compressedArray = bos.toByteArray();
    
    System.out.println("Byte array has been compressed!");
    System.out.println("Size of original array is:" + bytes.length);
    System.out.println("Size of compressed array is:" + compressedArray.length);
    
 }
}
 
/*
Output of this program would be
Byte array has been compressed!
Size of original array is:26
Size of compressed array is:18
*/
