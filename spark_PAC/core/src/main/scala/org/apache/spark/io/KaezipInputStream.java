package org.apache.spark.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.FilterInputStream;
import java.io.OutputStream;
//import java.util.zip.InflaterInputStream;



public class KaezipInputStream extends InputStream
{
    /**
     * stream to be decompressed
     */
    protected final KaeInflaterInputStream _inputStream;
    protected int buffer_size;

    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction
    ///////////////////////////////////////////////////////////////////////
     */

    public KaezipInputStream(final InputStream inputStream, int buffer_size) throws IOException
    {
        //this(inputStream, false);
        this.buffer_size = buffer_size;
        _inputStream = new KaeInflaterInputStream(inputStream, buffer_size);
    }

    
    @Override
    public int available() throws IOException
    {
      return _inputStream.available();
    }

    @Override
    public int read() throws IOException
    {
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
      return _inputStream.skip(n);
    }


    @Override
    public void close() throws IOException
    {
       _inputStream.close();
    }
}
