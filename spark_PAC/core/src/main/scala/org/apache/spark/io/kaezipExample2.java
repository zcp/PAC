package org.apache.spark.io;
import java.io.UnsupportedEncodingException;
import java.util.zip.DataFormatException;

public class kaezipExample2 {
    public static void main(String[] arg) throws DataFormatException, UnsupportedEncodingException  {
        String inStr = "Hellow World!,Hellow World,Hellow World,Hellow World,Hellow World!";
        byte[] data = inStr.getBytes("UTF-8");
        byte[] output = new byte[100];
        //Compresses the data
        KaeDeflater compresser = new KaeDeflater();
        compresser.setInput(data);
        compresser.finish();
        int bytesAfterdeflate = compresser.deflate(output);
        System.out.println("Compressed byte number:"+bytesAfterdeflate);
        //Decompresses the data
        KaeInflater decompresser = new KaeInflater();
        decompresser.setInput(output, 0, bytesAfterdeflate);
        byte[] result = new byte[100];
        int resultLength = decompresser.inflate(result);
        decompresser.end();
        String outStr = new String(result, 0, resultLength, "UTF-8");
        System.out.println("Decompressed data: "+outStr);
  }
}
