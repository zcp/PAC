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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.DataFormatException;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.ZipException;

import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class implements a stream filter for uncompressing data in the
 * "deflate" compression format. It is also used as the basis for other
 * decompression filters, such as GZIPInputStream.
 *
 * @see         //Inflater
 * @author      David Connelly
 */
public
class KaeInflaterInputStream extends FilterInputStream {
    /**
     * Decompressor for this stream.
     */
    protected KaeInflater inf;
    private static final Logger logger = LoggerFactory.getLogger(KaeInflaterInputStream.class);
    /**
     * Input buffer for decompression.
     */
    protected byte[] buf;

    /**
     * Length of input buffer.
     */
    protected int len;

    private boolean closed = false;
    // this flag is set to true after EOF has reached
    private boolean reachEOF = false;

    protected  String fileName = "";
    protected  BufferedWriter buffer_writer = null;
    protected FileOutputStream compression_tracer_fos = null;
    protected BufferedOutputStream bos = null;
    protected DeflaterOutputStream compression_tracer_outputStream = null;
    protected String rs_content = "";
    protected  String compression_performance_trace = SparkEnv.get().conf().get("spark.compression.performance.trace");

    protected long stream_id = System.nanoTime();
    /**
     * Check to make sure that this stream has not been closed
     */
    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException("Stream closed");
        }
    }


    /**
     * Creates a new input stream with the specified decompressor and
     * buffer size.
     * @param in the input stream
     * @param inf the decompressor ("inflater")
     * @param size the input buffer size
     * @exception IllegalArgumentException if {@code size <= 0}
     */
    public KaeInflaterInputStream(InputStream in, KaeInflater inf, int size) {
        // System.out.println("kaeInflaterInputStream is initialized");
        super(in);
        if (in == null || inf == null) {
            throw new NullPointerException();
        } else if (size <= 0) {
            throw new IllegalArgumentException("buffer size <= 0");
        }
        this.inf = inf;
        buf = new byte[size];
        //logger.info("zcp,KaeInflaterInputStream is initialized,buffer_size," + size);
        //System.out.println("KaeInflaterInputStream is initialized,buffer_size," + size);
        if(compression_performance_trace.equals("t")) {
            try {
                this.fileName = "/home/compression_tracer/inflate_details-" + this.stream_id;
                this.compression_tracer_fos = new FileOutputStream(this.fileName);
                this.bos = new BufferedOutputStream(this.compression_tracer_fos);
                this.compression_tracer_outputStream = new DeflaterOutputStream(this.bos);
                //this.buffer_writer = new BufferedWriter(new FileWriter(fileName));
                String content = "read_type, start_time(ns, relative to stream id),execution_time(ns), len \n";
                byte[] input = content.getBytes(StandardCharsets.UTF_8);
                this.compression_tracer_outputStream.write(input, 0, input.length);

                this.rs_content = "";

            } catch (Exception e) {
                System.out.println(e.toString());
                //this.buffer_writer = null;
                this.compression_tracer_fos = null;
                this.bos = null;
                this.compression_tracer_outputStream = null;
            }
        }
    }

    /**
     * Creates a new input stream with the specified decompressor and a
     * default buffer size.
     * @param in the input stream
     * @param inf the decompressor ("inflater")
     */
    public KaeInflaterInputStream(InputStream in, KaeInflater inf) {
        this(in, inf, 512);
    }

    boolean usesDefaultInflater = false;

    /**
     * Creates a new input stream with a default decompressor and buffer size.
     * @param in the input stream
     */
    public KaeInflaterInputStream(InputStream in) {
        this(in, new KaeInflater());
        usesDefaultInflater = true;
    }

    public KaeInflaterInputStream(InputStream in, int size) {
        this(in, new KaeInflater(), size);
        usesDefaultInflater = true;
    }

    private byte[] singleByteBuf = new byte[1];

    /**
     * Reads a byte of uncompressed data. This method will block until
     * enough input is available for decompression.
     * @return the byte read, or -1 if end of compressed input is reached
     * @exception IOException if an I/O error has occurred
     */
    public int read() throws IOException {
        ensureOpen();

        //long start_time = System.nanoTime();
        int len =  read(singleByteBuf, 0, 1) == -1 ? -1 : Byte.toUnsignedInt(singleByteBuf[0]);
        //long end_time = System.nanoTime();

        //if(this.buffer_writer != null && this.compression_performance_trace.equals("t")) {
            //long milliSec = System.currentTimeMillis();
            //convert milliseconds into Date by creating an object of the date class and passing milliseconds to the constructor
            //Date res = new Date(milliSec);
            //DateFormat sdf2 = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS Z");
        //    this.buffer_writer.write("R1," + stream_id + "," + (end_time - start_time)+ "," +start_time + "," + len+ "\n");
        //}
        return len;
    }

    /**
     * Reads uncompressed data into an array of bytes. If <code>len</code> is not
     * zero, the method will block until some input can be decompressed; otherwise,
     * no bytes are read and <code>0</code> is returned.
     * @param b the buffer into which the data is read
     * @param off the start offset in the destination array <code>b</code>
     * @param len the maximum number of bytes read
     * @return the actual number of bytes read, or -1 if the end of the
     *         compressed input is reached or a preset dictionary is needed
     * @exception  NullPointerException If <code>b</code> is <code>null</code>.
     * @exception  IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>b.length - off</code>
     * @exception ZipException if a ZIP format error has occurred
     * @exception IOException if an I/O error has occurred
     */
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        try {
            long start_time = System.nanoTime();
            int n;
            while ((n = inf.inflate(b, off, len)) == 0) {
                long end_time = System.nanoTime();
                if(this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
                    //long milliSec = System.currentTimeMillis();
                    //convert milliseconds into Date by creating an object of the date class and passing milliseconds to the constructor
                    //Date res = new Date(milliSec);
                    //DateFormat sdf2 = new SimpleDateFormat("dd MMM yyyy HH:mm:ss:SSS Z");
                    this.rs_content += "Rs,"  + (start_time - this.stream_id) + "," + (end_time - start_time) +  "," + n + "\n";
                    if(this.rs_content.length() > 100) {
                        byte[] input = this.rs_content.getBytes(StandardCharsets.UTF_8);
                        this.compression_tracer_outputStream.write(input, 0, input.length);
                        this.compression_tracer_outputStream.flush();
                        //this.buffer_writer.write();
                        this.rs_content = "";
                    }
                }
                if (inf.finished() || inf.needsDictionary()) {
                    reachEOF = true;
                    return -1;
                }
                if (inf.needsInput()) {
                    fill();
                }
                start_time = System.nanoTime();
            }

            long end_time = System.nanoTime();
            if (this.compression_tracer_fos != null && this.compression_performance_trace.equals("t")) {
                this.rs_content += "Rs," + (start_time - this.stream_id) + "," + (end_time - start_time) + "," + n + "\n";
                if(this.rs_content.length() > 100) {
                    byte[] input = this.rs_content.getBytes(StandardCharsets.UTF_8);
                    this.compression_tracer_outputStream.write(input, 0, input.length);
                    this.compression_tracer_outputStream.flush();
                    //this.buffer_writer.write();
                    this.rs_content = "";
                }
            }
            return n;
        } catch (DataFormatException e) {
            String s = e.getMessage();
            throw new ZipException(s != null ? s : "Invalid ZLIB data format");
        }
    }

    /**
     * Returns 0 after EOF has been reached, otherwise always return 1.
     * <p>
     * Programs should not count on this method to return the actual number
     * of bytes that could be read without blocking.
     *
     * @return     1 before EOF and 0 after EOF.
     * @exception  IOException  if an I/O error occurs.
     *
     */
    public int available() throws IOException {
        ensureOpen();
        if (reachEOF) {
            return 0;
        } else {
            return 1;
        }
    }

    private byte[] b = new byte[512];

    /**
     * Skips specified number of bytes of uncompressed data.
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped.
     * @exception IOException if an I/O error has occurred
     * @exception IllegalArgumentException if {@code n < 0}
     */
    public long skip(long n) throws IOException {
        if (n < 0) {
            throw new IllegalArgumentException("negative skip length");
        }
        ensureOpen();
        int max = (int)Math.min(n, Integer.MAX_VALUE);
        int total = 0;
        while (total < max) {
            int len = max - total;
            if (len > b.length) {
                len = b.length;
            }
            len = read(b, 0, len);
            if (len == -1) {
                reachEOF = true;
                break;
            }
            total += len;
        }
        return total;
    }

    /**
     * Closes this input stream and releases any system resources associated
     * with the stream.
     * @exception IOException if an I/O error has occurred
     */
    public void close() throws IOException {
        if (!closed) {
            if (usesDefaultInflater)
                inf.end();
            in.close();
            closed = true;
            if(this.compression_tracer_fos != null) {
                if(this.rs_content.length() > 0) {
                    byte[] input = this.rs_content.getBytes(StandardCharsets.UTF_8);
                    this.compression_tracer_outputStream.write(input, 0, input.length);
                    this.compression_tracer_outputStream.flush();
                    //this.buffer_writer.write();
                    this.rs_content = "";
                }
                this.compression_tracer_outputStream.close();
                this.bos.close();
                this.compression_tracer_fos = null;

            }
        }
    }

    /**
     * Fills input buffer with more data to decompress.
     * @exception IOException if an I/O error has occurred
     */
    protected void fill() throws IOException {
        ensureOpen();
        len = in.read(buf, 0, buf.length);
        if (len == -1) {
            throw new EOFException("Unexpected end of ZLIB input stream");
        }
        inf.setInput(buf, 0, len);
    }

    /**
     * Tests if this input stream supports the <code>mark</code> and
     * <code>reset</code> methods. The <code>markSupported</code>
     * method of <code>InflaterInputStream</code> returns
     * <code>false</code>.
     *
     * @return  a <code>boolean</code> indicating if this stream type supports
     *          the <code>mark</code> and <code>reset</code> methods.
     * @see     java.io.InputStream#mark(int)
     * @see     java.io.InputStream#reset()
     */
    public boolean markSupported() {
        return false;
    }

    /**
     * Marks the current position in this input stream.
     *
     * <p> The <code>mark</code> method of <code>InflaterInputStream</code>
     * does nothing.
     *
     * @param   readlimit   the maximum limit of bytes that can be read before
     *                      the mark position becomes invalid.
     * @see     java.io.InputStream#reset()
     */
    public synchronized void mark(int readlimit) {
    }

    /**
     * Repositions this stream to the position at the time the
     * <code>mark</code> method was last called on this input stream.
     *
     * <p> The method <code>reset</code> for class
     * <code>InflaterInputStream</code> does nothing except throw an
     * <code>IOException</code>.
     *
     * @exception  IOException  if this method is invoked.
     * @see     java.io.InputStream#mark(int)
     * @see     java.io.IOException
     */
    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }
}
