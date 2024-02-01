package org.apache.spark.io;

import org.apache.spark.SparkEnv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;


public class ZlibInputStream2 extends InputStream
{
    /**
     * stream to be decompressed
     */
    protected final ZlibInflaterInputStream2 _inputStream;
    protected int buffer_size;
    protected String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");


    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */

    public ZlibInputStream2(final InputStream inputStream, int buffer_size, long stream_id) throws IOException
    {
        //this(inputStream, false);

        long start_time = System.nanoTime();
        this.buffer_size = buffer_size;
        _inputStream = new ZlibInflaterInputStream2(inputStream, buffer_size, stream_id);
        long end_time = System.nanoTime();
        if (compression_performance_trace.equals("t")) {
            try {
                String content = "ZlibInflaterInputStream2 is created, stream_id, execution time(ns)," + stream_id + "," + (end_time - start_time);
                String fileName = "/home/compression_tracer/ZlibInflaterInputStream2-" + stream_id;
                BufferedWriter buffer_writer = new BufferedWriter(new FileWriter(fileName));
                buffer_writer.write(content + "\n");
                buffer_writer.close();
            } catch (Exception e) {
                System.out.println(e.toString());
            }
        }
    }
    
    @Override
    public int available() throws IOException
    {
      return _inputStream.available();
    }

    @Override
    public int read() throws IOException
    {
        //System.out.println("read is called");
        return _inputStream.read();
    }


    @Override
    public int read(final byte[] buffer, int offset, int length) throws IOException
    {
        return _inputStream.read(buffer, offset, length);
    }


    @Override
    public long skip(long n) throws IOException
    {

        //System.out.println("skip is called");
        return _inputStream.skip(n);
    }


    @Override
    public void close() throws IOException
    {
        //System.out.println("close is called");
       _inputStream.close();
    }
}
