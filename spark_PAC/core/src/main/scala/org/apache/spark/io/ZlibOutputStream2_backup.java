package org.apache.spark.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;


public class ZlibOutputStream2_backup extends OutputStream
{
    /**
     * stream to be compressed
     */
    protected final DeflaterOutputStream _outputStream;
    //delay buffer to compress
    //protected  byte[] b_delay;
    //protected  int b_delay_len;
    //protected int delay_buffersize;
    //protected long stream_id = 0;
    //protected String fileName  = "";
    //protected BufferedWriter buffer_writer = null;
    //protected String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");
    //protected  String compression_performance_trace = "false";
    /**
     * Output buffer for writing compressed data.
     */
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, configuration
    ///////////////////////////////////////////////////////////////////////
     */

    public ZlibOutputStream2_backup(final OutputStream outputStream, int buffer_size, long stream_id)
    {
        _outputStream = new DeflaterOutputStream(outputStream,new Deflater(),buffer_size);
        //this.b_delay = new byte[buffer_size];
        //this.b_delay_len = 0 ;
        //this.delay_buffersize = buffer_size;
        //this.stream_id = stream_id;
        /*
        if(compression_performance_trace == "true") {
            try {
                this.fileName = "/tmp/compression_tracer/deflate_details-" + this.stream_id;
                this.buffer_writer = new BufferedWriter(new FileWriter(fileName));
                String content = "type, stream_id, execution_time(ns), start_time(ns), input_len, output_len";
                this.buffer_writer.write(content + "\n");
            } catch (Exception e) {
                System.out.println(e.toString());
                this.buffer_writer = null;
            }
        }

         */
    }


    /*
    ///////////////////////////////////////////////////////////////////////
    // OutputStream impl
    ///////////////////////////////////////////////////////////////////////
     */
    /*
    public void cache_copy(byte[] b, int off, int len) throws IOException {
        long start_time = System.nanoTime();
        System.arraycopy(b,off,this.b_delay,this.b_delay_len,len);
        this.b_delay_len += len;
        //for(int i = 0; i < len; i++)
        //    this.b_delay[this.b_delay_len++] = b[off + i];
        long end_time = System.nanoTime();

        if (this.buffer_writer != null && this.compression_performance_trace == "true") {
            this.buffer_writer.write("cp," + stream_id + "," + (end_time - start_time) +
                    "," + start_time + "," + len  + "," + "no_know" +"\n");
        }
    }

    public void clearBDelay(){
        Arrays.fill(this.b_delay, (byte) 0);
        //for(int i = 0; i < this.b_delay.length; i++)
        //    b_delay[i] = 0;
        this.b_delay_len = 0;
    }
*/
    @Override
    public void write(final int singleByte) throws IOException
    {
        System.out.println("write_singleByte is called");
        _outputStream.write(singleByte);
    }

    @Override
    public void write(final byte[] buffer, int offset, int length) throws IOException {
        System.out.println("write byte array is called");
        _outputStream.write(buffer, offset, length);
        /*
        int available_buffer = this.delay_buffersize - this.b_delay_len;
        System.out.println("1,offset, length," + offset + "," + length);
        long start_time = System.nanoTime();
        if (available_buffer > length) {
            // copy byte to delay_buffer
            this.copy(buffer, offset, length);
            //System.out.println("2");
        }
        else{
            System.out.println("3");
            _outputStream.write(this.b_delay, 0, this.b_delay_len);
            System.out.println("write,b_delay_len,"+this.b_delay_len);
            System.out.println("4");
            this.clearBDelay();
            System.out.println("5");
            //_outputStream.write(buffer,offset, length);
            this.copy(buffer,offset,length);
            System.out.println("6");
            long end_time = System.nanoTime();
            if (this.buffer_writer != null && this.compression_performance_trace == "true") {
                this.buffer_writer.write("JNI_deflate," + stream_id + "," + (end_time - start_time) +
                            "," + start_time + "," + length  + "," + "no_known" + "\n");
                }
        }*/

    }


    @Override
    public void flush() throws IOException
    {
        System.out.println("flush");
        /*if(this.b_delay_len > 0) {
            System.out.println("flush,b_delay_len,"+this.b_delay_len);
            _outputStream.write(this.b_delay, 0, this.b_delay_len);
            //this.b_delay_len = 0;
            this.clearBDelay();
        }

         */
         _outputStream.flush();
    }

    @Override
    public void close() throws IOException
    {
        System.out.println("close");

       _outputStream.close();
       // if(this.buffer_writer != null)
       //     this.buffer_writer.close();
    }


    public void finish() throws IOException
    {
        System.out.println("finish");
       _outputStream.finish();
    }

}
