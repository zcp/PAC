package org.apache.spark.io;

import org.apache.spark.SparkEnv;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;


public class ZlibInputStream2_backup extends InputStream
{
    /**
     * stream to be decompressed
     */
    protected final InflaterInputStream _inputStream;

    private byte[] default_decompress_buffer;
    private int default_decompress_buffer_len = 0;
    private int default_decompress_buffer_off = 0;
    private int default_decompress_buffer_elements = 0;

    protected long stream_id = 0;
    protected String fileName  = "";
    protected BufferedWriter buffer_writer;
    protected String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */

    public ZlibInputStream2_backup(final InputStream inputStream, int buffer_size, long stream_id) throws IOException
    {
        //this(inputStream, false);
        _inputStream = new InflaterInputStream(inputStream, new Inflater(), buffer_size);
        this.stream_id = stream_id;
        this.default_decompress_buffer = new byte[buffer_size];
        this.default_decompress_buffer_off = 0;
        this.default_decompress_buffer_elements = 0;
        this.default_decompress_buffer_len = buffer_size;

        if(compression_performance_trace.equals("t")) {
            try {
                this.fileName = "/home/compression_tracer/inflate_details-" + this.stream_id;
                this.buffer_writer = new BufferedWriter(new FileWriter(fileName));
                String content = "read_type,stream_id, execution time(ns),start_time(ns),len";
                this.buffer_writer.write(content + "\n");
            } catch (Exception e) {
                System.out.println(e.toString());
                this.buffer_writer = null;
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


    public int copy(byte[] b, int off, int len) throws IOException {
        //System.out.println("copy is called");
        long start_time = System.nanoTime();
        int b_off = off;

        if(len == 0)
            return 0;

        int remaining_elements = this.default_decompress_buffer_elements - this.default_decompress_buffer_off;
        if(remaining_elements <= 0) {
            //System.out.println("remaining_elements is zero, len," + len);
            return 0;
        }
        int num = len < remaining_elements ? len : remaining_elements;
        System.arraycopy(this.default_decompress_buffer, this.default_decompress_buffer_off,
                         b, off, num);
        this.default_decompress_buffer_off += num;
        //for(int i = 0; i < num; i++)
        //    b[b_off++] = this.default_decompress_buffer[this.default_decompress_buffer_off++];

        long end_time = System.nanoTime();
        if (this.buffer_writer != null && this.compression_performance_trace.equals("t")) {
            this.buffer_writer.write("cp," + stream_id + "," + (end_time - start_time) + "," + start_time + ","+num + "\n");
        }
        return num;
    }

    @Override
    public int read(final byte[] buffer, int offset, int length) throws IOException
    {

        int total_num = 0;
        int num =  this.copy(buffer,offset,length);
        System.out.println("read byte array is called, length,n," + length + "," + num);
        offset += num;
        length -= num;
        total_num += num;
        if (length == 0) {
            return total_num;
        }
        while(length > 0) {
            long start_time = System.nanoTime();
            this.default_decompress_buffer_off = 0;
            int pre_length = _inputStream.read(this.default_decompress_buffer,
                                               this.default_decompress_buffer_off,
                                               this.default_decompress_buffer_len);
            if(pre_length >= 0)
               this.default_decompress_buffer_elements = pre_length;
            else
                this.default_decompress_buffer_elements = 0;

            long end_time = System.nanoTime();
            if (this.buffer_writer != null && this.compression_performance_trace.equals("t")) {
                this.buffer_writer.write("pre_read," + stream_id + "," + (end_time - start_time) + ","
                                              + start_time + "," + pre_length +"\n");
            }

            if(pre_length == -1){
                this.default_decompress_buffer_off = 0;
                this.default_decompress_buffer_elements = 0;

                if(total_num !=0)
                    return total_num;
                else
                    return -1;
            }

            num = this.copy(buffer,offset, length);
            System.out.println("read byte array is called, length,n," + length + "," + num);
            total_num += num;
            offset += num;
            length -= num;
        }
        return total_num;
        //return _inputStream.read(buffer, offset, length);
    }


    @Override
    public long skip(long n) throws IOException
    {
        System.out.println("skip is called");
      return _inputStream.skip(n);
    }


    @Override
    public void close() throws IOException
    {
        System.out.println("close is called");
       _inputStream.close();
       if(this.buffer_writer != null)
           this.buffer_writer.close();
    }
}
