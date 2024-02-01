package org.apache.spark.io;

import org.apache.spark.SparkEnv;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;


public class ZlibOutputStream extends OutputStream
{
    /**
     * stream to be compressed
     */
    protected final DeflaterOutputStream _outputStream;
    protected long stream_id = 0;
    protected String fileName  = "";
    protected FileOutputStream compression_tracer_fos = null;
    protected BufferedOutputStream bos = null;
    protected DeflaterOutputStream compression_tracer_outputStream = null;
    protected String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, configuration
    ///////////////////////////////////////////////////////////////////////
     */

    public ZlibOutputStream(final OutputStream outputStream, int buffer_size)
    {
        _outputStream = new DeflaterOutputStream(outputStream,new Deflater(), buffer_size);
        this.stream_id = System.nanoTime();

        if(compression_performance_trace.equals("t")) {
            try {
                this.fileName = "/home/compression_tracer/deflate_details-" + this.stream_id;
                this.compression_tracer_fos = new FileOutputStream(this.fileName);
                this.bos = new BufferedOutputStream(this.compression_tracer_fos);
                //this.buffer_writer = new BufferedWriter(new FileWriter(fileName));
                this.compression_tracer_outputStream = new DeflaterOutputStream(this.bos);
                String content = "type, execution_time(ns), start_time(ns,relative to stream id), input_len, output_len \n";
                byte[] input = content.getBytes(StandardCharsets.UTF_8);
                this.compression_tracer_outputStream.write(input, 0, input.length);
                //this.compression_tracer_outputStream.close();
                //this.buffer_writer.write(content + "\n");
            } catch (Exception e) {
                System.out.println(e.toString());
                this.compression_tracer_fos = null;
                this.bos = null;
                this.compression_tracer_outputStream = null;
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
        //System.out.println("write_singleByte is called");
        _outputStream.write(singleByte);
    }

    @Override
    public void write(final byte[] buffer, int offset, int length) throws IOException
    {
        //System.out.println("write byte array is called," + length);
        long start_time = System.nanoTime();
        _outputStream.write(buffer, offset, length);
        long end_time = System.nanoTime();
        if (this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
            String content = "Jdf,"  + (end_time - start_time) + "," + (start_time - this.stream_id)+ "," + length  + "," + "-1" + "\n";
            //System.out.println(content);
            byte[] input = content.getBytes(StandardCharsets.UTF_8);
            this.compression_tracer_outputStream.write(input, 0, input.length);
            //this.buffer_writer.write();
        }
    }


    @Override
    public void flush() throws IOException
    {
        //System.out.println("flush");
        _outputStream.flush();
    }

    @Override
    public void close() throws IOException
    {
        //System.out.println("close");
        _outputStream.close();
        if(this.compression_tracer_fos != null) {
            //this.compression_tracer_fos.close();
            this.compression_tracer_outputStream.close();
        }
    }


    public void finish() throws IOException
    {
        //System.out.println("finish");
        _outputStream.finish();
    }

}
