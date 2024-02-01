package org.apache.spark.io;

import org.apache.spark.SparkEnv;

import java.io.*;
import java.util.zip.DeflaterOutputStream;
//import java.util.zip.DeflaterOutputStream;


public class KaezipOutputStream2 extends OutputStream
{
    /**
     * stream to be compressed
     */
    // protected final KaeDeflaterOutputStream2 _outputStream2;
    protected final KaeDeflaterOutputStream2 _outputStream;
    protected final int buffer_size;
    protected long total_execution_time = 0;
    protected String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, configuration
    ///////////////////////////////////////////////////////////////////////
     */

    public KaezipOutputStream2(final OutputStream outputStream, int buff_size, long stream_id) {
        this.buffer_size = buff_size;
        //System.out.println("zcp,buffer_size," + buff_size);
        long start_time = System.nanoTime();
        _outputStream = new KaeDeflaterOutputStream2(outputStream, buff_size, stream_id);
        long end_time = System.nanoTime();
        if (compression_performance_trace.equals("t")){
            try {
                String content = "KaeDeflaterOutputStream2 is created, stream_id, execution time(ns)," + stream_id + "," + (end_time - start_time);
                String fileName = "/home/compression_tracer/KaeDeflaterOutputStream2-" + stream_id;
                BufferedWriter buffer_writer = new BufferedWriter(new FileWriter(fileName));
                buffer_writer.write(content + "\n");
                buffer_writer.close();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }


    /*
    ///////////////////////////////////////////////////////////////////////
    // OutputStream impl
    ///////////////////////////////////////////////////////////////////////
     */

    @Override
    public void write(final int singleByte) throws IOException
    {
        _outputStream.write(singleByte);
    }

    @Override
    public void write(final byte[] buffer, int offset, int length) throws IOException
    {
        _outputStream.write(buffer, offset, length);
    }


    @Override
    public void flush() throws IOException
    {
         _outputStream.flush();
    }

    @Override
    public void close() throws IOException
    {
       _outputStream.close();

    }


    public void finish() throws IOException
    {
        _outputStream.finish();
    }

}
