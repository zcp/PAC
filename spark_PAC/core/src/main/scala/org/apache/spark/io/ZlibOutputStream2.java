package org.apache.spark.io;

import org.apache.spark.SparkEnv;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;


public class ZlibOutputStream2 extends OutputStream
{
    /**
     * stream to be compressed
     */
    protected final DeflaterOutputStream _outputStream;
    //delay buffer to compress
    protected  byte[] b_delay;
    protected  int b_delay_len;
    protected int delay_buffersize;
    protected long stream_id = 0;
    protected String fileName  = "";
    protected FileOutputStream compression_tracer_fos = null;
    protected BufferedOutputStream bos = null;
    protected DeflaterOutputStream compression_tracer_outputStream = null;

    protected String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");

    /**
     * Output buffer for writing compressed data.
     */
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, configuration
    ///////////////////////////////////////////////////////////////////////
     */

    public ZlibOutputStream2(final OutputStream outputStream, int buffer_size, long stream_id)
    {
        // Creates a new compressor using compression level 1, the fastest level.
        Deflater deflater = new Deflater(1);
        _outputStream = new DeflaterOutputStream(outputStream,deflater,buffer_size);
        this.b_delay = new byte[buffer_size];
        this.b_delay_len = 0 ;
        this.delay_buffersize = buffer_size;
        this.stream_id = stream_id;

        if(compression_performance_trace.equals("t")) {
            try {
                this.fileName = "/home/compression_tracer/deflate_details-" + this.stream_id;
                this.compression_tracer_fos = new FileOutputStream(new File(this.fileName));
                //this.buffer_writer = new BufferedWriter(new FileWriter(fileName));
                this.bos = new BufferedOutputStream(this.compression_tracer_fos);
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

    public void copy(byte[] b, int off, int len) throws IOException {
        long start_time = System.nanoTime();
        System.arraycopy(b,off,this.b_delay,this.b_delay_len,len);
        this.b_delay_len += len;
        //for(int i = 0; i < len; i++)
        //    this.b_delay[this.b_delay_len++] = b[off + i];
        long end_time = System.nanoTime();

        if (this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
            String content = "cp," + (end_time - start_time) +
                    "," + (start_time - this.stream_id) + "," + len  + "," + "-1" +"\n";
            byte[] input = content.getBytes(StandardCharsets.UTF_8);
            this.compression_tracer_outputStream.write(input, 0, input.length);
            //this.buffer_writer.write("cp," + (end_time - start_time) +
            //        "," + (start_time - this.stream_id) + "," + len  + "," + "no_know" +"\n");
        }
    }

    public void clearBDelay(){
        Arrays.fill(this.b_delay, (byte) 0);
        //for(int i = 0; i < this.b_delay.length; i++)
        //    b_delay[i] = 0;
        this.b_delay_len = 0;
    }

    @Override
    public void write(final int singleByte) throws IOException
    {
        //System.out.println("write_singleByte is called");
        _outputStream.write(singleByte);
    }

    @Override
    public void write(final byte[] buffer, int offset, int length) throws IOException {
        //System.out.println("write byte array is called");

        int available_buffer = this.delay_buffersize - this.b_delay_len;
        //System.out.println("1,offset, length," + offset + "," + length);
        long start_time = System.nanoTime();
        if (available_buffer > length) {
            // copy byte to delay_buffer
            this.copy(buffer, offset, length);
            //System.out.println("2");
        }
        else{
            _outputStream.write(this.b_delay, 0, this.b_delay_len);
            //System.out.println("write,b_delay_len,"+this.b_delay_len);
            this.clearBDelay();
            //_outputStream.write(buffer,offset, length);
            this.copy(buffer,offset,length);
            long end_time = System.nanoTime();
            if (this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
                String content = "Jdf,"  + (end_time - start_time) +
                        "," + (start_time - this.stream_id)+ "," + length  + "," + "-1" + "\n";
                //System.out.println(content);
                byte[] input = content.getBytes(StandardCharsets.UTF_8);
                this.compression_tracer_outputStream.write(input, 0, input.length);
                //this.buffer_writer.write();
                }
        }

        //_outputStream.write(buffer, offset, length);
    }


    @Override
    public void flush() throws IOException
    {
        //System.out.println("flush");
        if(this.b_delay_len > 0) {
            //System.out.println("flush,b_delay_len,"+this.b_delay_len);
            _outputStream.write(this.b_delay, 0, this.b_delay_len);
            //this.b_delay_len = 0;
            this.clearBDelay();
        }

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
