package org.apache.spark.io;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
//import java.util.zip.DeflaterOutputStream;


public class KaezipOutputStream extends OutputStream
{
    /**
     * stream to be compressed
     */
    // protected final KaeDeflaterOutputStream2 _outputStream2;
    protected final KaeDeflaterOutputStream _outputStream;
    protected final int buffer_size;
    /*
    ///////////////////////////////////////////////////////////////////////
    // Construction, configuration
    ///////////////////////////////////////////////////////////////////////
     */

    public KaezipOutputStream(final OutputStream outputStream, int buff_size)
    {
        this.buffer_size = buff_size;
        _outputStream = new KaeDeflaterOutputStream(outputStream, buffer_size);

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
