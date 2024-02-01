package org.apache.spark.io;

import org.apache.spark.SparkEnv;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;



public class ZlibInputStream extends InputStream
{
    /**
     * stream to be decompressed
     */
    protected final InflaterInputStream _inputStream;

    protected long stream_id = 0;
    protected String fileName  = "";
    //protected BufferedWriter buffer_writer;
    protected FileOutputStream compression_tracer_fos = null;
    protected BufferedOutputStream bos = null;
    protected DeflaterOutputStream compression_tracer_outputStream = null;

    protected String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");


    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */

    public ZlibInputStream(final InputStream inputStream, int buffer_size) throws IOException
    {
        //this(inputStream, false);
        _inputStream = new InflaterInputStream(inputStream, new Inflater(), buffer_size);

        this.stream_id = System.nanoTime();

        if(compression_performance_trace.equals("t")) {
            try {
                this.fileName = "/home/compression_tracer/inflate_details-" + this.stream_id;
                this.compression_tracer_fos = new FileOutputStream(new File(this.fileName));
                this.bos = new BufferedOutputStream(this.compression_tracer_fos);
                //this.buffer_writer = new BufferedWriter(new FileWriter(fileName));
                this.compression_tracer_outputStream = new DeflaterOutputStream(bos);


                //this.buffer_writer = new BufferedWriter(new FileWriter(fileName));
                String content = "read_type, execution time(ns),start_time(ns,relative to stream id),len \n";
                byte[] input = content.getBytes(StandardCharsets.UTF_8);
                this.compression_tracer_outputStream.write(input, 0, input.length);
                //this.buffer_writer.write(content + "\n");
            } catch (Exception e) {
                System.out.println(e.toString());
                this.compression_tracer_fos = null;
                this.bos = null;
                this.compression_tracer_outputStream = null;
                //this.buffer_writer = null;
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
        //System.out.println("read is called" );
        int n = _inputStream.read();
        return n;
    }

    @Override
    public int read(final byte[] buffer, int offset, int length) throws IOException
    {
        long start_time = System.nanoTime();
        int n = _inputStream.read(buffer, offset, length);
        long end_time = System.nanoTime();
        if (this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
            String content = "r," + (end_time - start_time) + "," + (start_time - this.stream_id) + "," + n +"\n";
            byte[] input = content.getBytes(StandardCharsets.UTF_8);
            this.compression_tracer_outputStream.write(input,0,input.length);
        }

        //System.out.println("read byte array is called,length,n," + length + "," + n);
        return n;
    }


    @Override
    public long skip(long n) throws IOException
    {
        //System.out.println("skip is called,n," + n);
      return _inputStream.skip(n);
    }


    @Override
    public void close() throws IOException
    {
        //System.out.println("close is called,n,");
       _inputStream.close();
        if(this.compression_tracer_fos != null) {
            //this.compression_tracer_fos.close();
            this.compression_tracer_outputStream.close();
        }
    }
}
