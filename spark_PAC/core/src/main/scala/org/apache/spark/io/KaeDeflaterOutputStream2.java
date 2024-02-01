/*
 * Copyright (c) 1996, 2013, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

//package java.util.zip;
package org.apache.spark.io;

import org.apache.spark.SparkEnv;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.zip.DeflaterOutputStream;

/**
 * This class implements an output stream filter for compressing data in
 * the "deflate" compression format. It is also used as the basis for other
 * types of compression filters, such as GZIPOutputStream.
 *
 * @see         Deflater
 * @author      David Connelly
 */
public
class KaeDeflaterOutputStream2 extends FilterOutputStream {
    /**
     * Compressor for this stream.
     */
    protected KaeDeflater def;

    // a buffer is used to delay compression for kaezip
    protected  byte[] b_delay;
    protected  int b_delay_len;
    /**
     * Output buffer for writing compressed data.
     */
    protected byte[] buf;
    protected int buf_len;
    protected long stream_id = 0;
    protected String fileName  = "";
    protected BufferedWriter buffer_writer;
    protected FileOutputStream compression_tracer_fos = null;
    protected BufferedOutputStream bos = null;
    protected DeflaterOutputStream compression_tracer_outputStream = null;
    protected String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");


    /**
     * Indicates that the stream has been closed.
     */

    private boolean closed = false;

    private final boolean syncFlush;

    /**
     * Creates a new output stream with the specified compressor,
     * buffer size and flush mode.

     * @param out the output stream
     * @param def the compressor ("deflater")
     * @param size the output buffer size
     * @param syncFlush
     *        if {@code true} the {@link #flush()} method of this
     *        instance flushes the compressor with flush mode
     *        {@link Deflater#SYNC_FLUSH} before flushing the output
     *        stream, otherwise only flushes the output stream
     *
     * @throws IllegalArgumentException if {@code size <= 0}
     *
     * @since 1.7
     */
    public KaeDeflaterOutputStream2(OutputStream out,
                                    KaeDeflater def,
                                    int size,
                                    boolean syncFlush) {
        super(out);
        if (out == null || def == null) {
            throw new NullPointerException();
        } else if (size <= 0) {
            throw new IllegalArgumentException("buffer size <= 0");
        }
        this.def = def;
        this.buf = new byte[size];
        this.buf_len = 0;
        this.b_delay = new byte[size];
        this.b_delay_len = 0 ;
        this.syncFlush = syncFlush;

        //System.out.println("KaeDeflaterOutputStream2 is initialized,buffer_size," + size);
    }


    /**
     * Creates a new output stream with the specified compressor and
     * buffer size.
     *
     * <p>The new output stream instance is created as if by invoking
     * the 4-argument constructor DeflaterOutputStream(out, def, size, false).
     *
     * @param out the output stream
     * @param def the compressor ("deflater")
     * @param size the output buffer size
     * @exception IllegalArgumentException if {@code size <= 0}
     */
    public KaeDeflaterOutputStream2(OutputStream out, KaeDeflater def, int size) throws IOException {
        this(out, def, size, false);
    }

    /**
     * Creates a new output stream with the specified compressor, flush
     * mode and a default buffer size.
     *
     * @param out the output stream
     * @param def the compressor ("deflater")
     * @param syncFlush
     *        if {@code true} the {@link #flush()} method of this
     *        instance flushes the compressor with flush mode
     *        {@link Deflater#SYNC_FLUSH} before flushing the output
     *        stream, otherwise only flushes the output stream
     *
     * @since 1.7
     */
    public KaeDeflaterOutputStream2(OutputStream out,
                                    KaeDeflater def,
                                    boolean syncFlush) {
        this(out, def, 512, syncFlush);
    }


    /**
     * Creates a new output stream with the specified compressor and
     * a default buffer size.
     *
     * <p>The new output stream instance is created as if by invoking
     * the 3-argument constructor DeflaterOutputStream(out, def, false).
     *
     * @param out the output stream
     * @param def the compressor ("deflater")
     */
    public KaeDeflaterOutputStream2(OutputStream out, KaeDeflater def) {
        this(out, def, 512, false);
    }

    boolean usesDefaultDeflater = false;


    /**
     * Creates a new output stream with a default compressor, a default
     * buffer size and the specified flush mode.
     *
     * @param out the output stream
     * @param syncFlush
     *        if {@code true} the {@link #flush()} method of this
     *        instance flushes the compressor with flush mode
     *        {@link Deflater#SYNC_FLUSH} before flushing the output
     *        stream, otherwise only flushes the output stream
     *
     * @since 1.7
     */
    public KaeDeflaterOutputStream2(OutputStream out, boolean syncFlush)  {
        this(out, new KaeDeflater(), 512, syncFlush);
        usesDefaultDeflater = true;
    }

    /**
     * Creates a new output stream with a default compressor and buffer size.
     *
     * <p>The new output stream instance is created as if by invoking
     * the 2-argument constructor DeflaterOutputStream(out, false).
     *
     * @param out the output stream
     */
    public KaeDeflaterOutputStream2(OutputStream out) {
        this(out, false);
        usesDefaultDeflater = true;
    }


    public KaeDeflaterOutputStream2(OutputStream out, int size, long stream_id) {
        this(out, new KaeDeflater(), size, false);
        usesDefaultDeflater = true;
        this.stream_id = stream_id;
        if(compression_performance_trace.equals("t")) {
            try {
                this.fileName = "/home/compression_tracer/deflate_details-" + this.stream_id;
                this.compression_tracer_fos = new FileOutputStream(this.fileName);
                this.bos = new BufferedOutputStream(this.compression_tracer_fos);
                this.compression_tracer_outputStream = new DeflaterOutputStream(this.bos);
                //this.buffer_writer = new BufferedWriter(new FileWriter(fileName));
                String content = "type, start_time(ns, relative to stream id),execution_time(ns), input_len, output_len \n";
                byte[] input = content.getBytes(StandardCharsets.UTF_8);
                this.compression_tracer_outputStream.write(input, 0, input.length);

                //this.buffer_writer.write(content + "\n");
            } catch (Exception e) {
                System.out.println(e.toString());
                //this.buffer_writer = null;
                this.compression_tracer_fos = null;
                this.bos = null;
                this.compression_tracer_outputStream = null;
            }
        }

        //System.out.println("zcp, here is ok");
    }
    /**
     * Writes a byte to the compressed output stream. This method will
     * block until the byte can be written.
     * @param b the byte to be written
     * @exception IOException if an I/O error has occurred
     */
    public void write(int b) throws IOException {
        //System.out.println("write(int b) is called");
        //System.out.println((byte)(b & 0xff));

        byte[] buf = new byte[1];
        buf[0] = (byte)(b & 0xff);
        write(buf, 0, 1);
    }

    public void copy(byte[] b, int off, int len) throws IOException {
        // System.out.println("b, off,len, b_delay_len++," + b.length +"," + off + "," +len + "," + this.b_delay_len);
        long start_time = System.nanoTime();
        System.arraycopy(b,off,this.b_delay,this.b_delay_len,len);
        this.b_delay_len += len;
        //for(int i = 0; i < len; i++)
        //    this.b_delay[this.b_delay_len++] = b[off + i];

        long end_time = System.nanoTime();
        if (this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
            String content = "cp,"  + (start_time-this.stream_id) + "," + (end_time - start_time) + "," + len + "," + len  +"\n";
            byte[] input = content.getBytes(StandardCharsets.UTF_8);
            this.compression_tracer_outputStream.write(input, 0, input.length);
            //this.buffer_writer.write();
        }
    }

    public void clearBDelay(){
        for(int i = 0; i < b_delay.length; i++)
            b_delay[i] = 0;
        this.b_delay_len = 0;
    }

    /**
     * Writes an array of bytes to the compressed output stream. This
     * method will block until all the bytes are written.
     * @param b the data to be written
     * @param off the start offset of the data
     * @param len the length of the data
     * @exception IOException if an I/O error has occurred
     */
    public void write(byte[] b, int off, int len) throws IOException {
        //System.out.println("write(byte[] b) is called");
        // System.out.println("byte[] b," + ",off," + off +",len," + len + ',' + b.length + ',' + Arrays.toString(b) );

        if (def.finished()) {
            throw new IOException("write beyond end of stream");
        }
        if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        if (!def.finished()) {
            int threshold = (int) (b_delay.length * 0.9);
            // if buf_delay has enough space to store b, copy b to buf_delay
            if((b_delay.length - this.b_delay_len) > len) {
                //copy b to buf_delay
                copy(b, off, len);

                /*
                if (this.b_delay_len > threshold) {
                    def.setInput(b_delay, 0, this.b_delay_len);
                    start_time = System.nanoTime();
                    while (!def.needsInput()) {
                        //System.out.println("deflate is called");
                        deflate();
                    }
                    end_time = System.nanoTime();
                    this.buffer_writer.write("b_delay, deflate is called, execution time(ns)," + (end_time - start_time));
                    start_time = System.nanoTime();
                    copy(b, off, len);
                    end_time = System.nanoTime();
                    this.buffer_writer.write("copy is called, execution time(ns)," + (end_time - start_time));
                    //init buf_delay
                    clearBDelay();
                }

                 */
            }
            else {
                // compress b_delay firstly
                def.setInput(b_delay, 0, this.b_delay_len);
                while (!def.needsInput()) {
                    //System.out.println("delate is called");
                    long start_time = System.nanoTime();
                    deflate();
                    long end_time = System.nanoTime();
                    if(this.compression_tracer_fos != null&& this.compression_performance_trace.equals("t")) {
                        String content = "deflate_delay,"  + (start_time - this.stream_id) + ',' +
                                         (end_time - start_time) + "," + this.b_delay_len + "," + "-1" + "\n";
                        byte[] input = content.getBytes(StandardCharsets.UTF_8);
                        this.compression_tracer_outputStream.write(input, 0, input.length);
                    }
                }
                //init buf_delay
                clearBDelay();
                // if new data in b is less than the half of b_delay, copy it to b_delay
                // otherwise, we compress new data directly
                if(len < (b_delay.length - this.b_delay_len)/2) {
                    //System.out.println("b, off,len," + b.length +"," + off + "," +len);
                    copy(b, off, len);
                }
                else{
                    def.setInput(b, 0, len);
                    while (!def.needsInput()) {
                        //System.out.println("delate is called");
                        long start_time = System.nanoTime();
                        deflate();
                        long end_time = System.nanoTime();
                        if(this.compression_tracer_fos != null&& this.compression_performance_trace.equals("t")) {
                            String content = "deflate_direct," + (start_time - this.stream_id) + "," +
                                             (end_time - start_time)+ "," + this.b_delay_len + "," + "-1" + "\n";
                            byte[] input = content.getBytes(StandardCharsets.UTF_8);
                            this.compression_tracer_outputStream.write(input, 0, input.length);
                            //this.buffer_writer.write();
                        }
                    }
                }
            }
        }
    }

    /**
     * Finishes writing compressed data to the output stream without closing
     * the underlying stream. Use this method when applying multiple filters
     * in succession to the same output stream.
     * @exception IOException if an I/O error has occurred
     */
    public void finish() throws IOException {
        if (!def.finished()) {
            def.finish();
            def.setInput(b_delay, 0, this.b_delay_len);

            while (!def.finished()) {
                //System.out.println("finish() is called");
                long start_time = System.nanoTime();
                deflate();
                long end_time = System.nanoTime();
                if(this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
                    String content = "finish_deflate," + (start_time - this.stream_id) + "," +
                                     (end_time - start_time) + "," + this.b_delay_len + "," + "-1" + "\n";
                    byte[] input = content.getBytes(StandardCharsets.UTF_8);
                    this.compression_tracer_outputStream.write(input, 0, input.length);
                    //this.buffer_writer.write();
                }
            }

        }
    }

    /**
     * Writes remaining compressed data to the output stream and closes the
     * underlying stream.
     * @exception IOException if an I/O error has occurred
     */
    public void close() throws IOException {
        if (!closed) {
            finish();
            if (usesDefaultDeflater)
                def.end();
            out.close();
            closed = true;
            if(this.compression_tracer_fos != null) {
                //this.buffer_writer.close();
                this.compression_tracer_outputStream.close();
            }
        }
    }

    /**
     * Writes next block of compressed data to the output stream.
     * @throws IOException if an I/O error has occurred
     */
    protected void deflate() throws IOException {
        long start_time = System.nanoTime();
        int len = def.deflate(buf, 0, buf.length);
        long end_time = System.nanoTime();
        if(this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
            //long milliSec = System.currentTimeMillis();
            //convert milliseconds into Date by creating an object of the date class and passing milliseconds to the constructor
            //Date res = new Date(milliSec);
            //DateFormat sdf2 = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS Z");
            String content = "JNI_deflate," + (start_time - this.stream_id) + "," +
                              (end_time - start_time) + "," + buf.length + "," + len + "\n";
            byte[] input = content.getBytes(StandardCharsets.UTF_8);
            this.compression_tracer_outputStream.write(input, 0, input.length);
            //this.buffer_writer.write();
        }
        if (len > 0) {
            out.write(buf, 0, len);
        }
    }

    /**
     * Flushes the compressed output stream.
     *
     * If {@link #DeflaterOutputStream(OutputStream, Deflater, int, boolean)
     * syncFlush} is {@code true} when this compressed output stream is
     * constructed, this method first flushes the underlying {@code compressor}
     * with the flush mode {@link Deflater#SYNC_FLUSH} to force
     * all pending data to be flushed out to the output stream and then
     * flushes the output stream. Otherwise this method only flushes the
     * output stream without flushing the {@code compressor}.
     *
     * @throws IOException if an I/O error has occurred
     *
     * @since 1.7
     */
    public void flush() throws IOException {
        //System.out.println("flush is called");

        if (syncFlush && !def.finished()) {
            long start_time = System.nanoTime();
            int total_bytes = 0;
            int len = 0;
            while ((len = def.deflate(buf, 0, buf.length, KaeDeflater.SYNC_FLUSH)) > 0)
            {
                out.write(buf, 0, len);
                total_bytes += len;
                if (len < buf.length)
                    break;
            }
            long end_time = System.nanoTime();
            if(this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
                String content = "flush_deflate," + (start_time - this.stream_id) + "," +
                                 (end_time - start_time)+  "," + buf.length + ","  + total_bytes + "\n";
                byte[] input = content.getBytes(StandardCharsets.UTF_8);
                this.compression_tracer_outputStream.write(input, 0, input.length);
                //this.buffer_writer.write();
            }
        }
        /*
        //compress the remaining bytes
        System.out.println("b_delay_len," + b_delay_len);
        if(this.b_delay_len > 0){
            if (!def.finished()) {
                def.setInput(b_delay, 0, this.b_delay_len);
                int len = 0;
                while ((len = def.deflate(buf, 0, buf.length, KaeDeflater.SYNC_FLUSH)) > 0)
                {
                    out.write(buf, 0, len);
                    System.out.println("len,length," + "," + len + "," + buf.length);
                    if (len < buf.length)
                        break;
                }
            }
            clearBDelay();
        }
         */
        out.flush();
    }
}
