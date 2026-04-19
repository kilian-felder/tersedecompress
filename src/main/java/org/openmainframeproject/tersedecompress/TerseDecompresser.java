package org.openmainframeproject.tersedecompress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;

abstract class TerseDecompresser implements AutoCloseable
{
	TerseBlockReader input;
	ByteArrayOutputStream record;
	DataOutputStream stream;
	
	boolean HostFlag; 
	boolean TextFlag;
	boolean VariableFlag;
	
    long         OutputTotal   = 0    ; /* total number of bytes                    */
    int         RecordLength; /* host perspective record length           */
	
    byte[] lineseparator = System.lineSeparator().getBytes();

    /* Sentinel object used to signal end-of-stream in the record queue */
    private static final byte[] RECORD_EOF = new byte[0];

    private LinkedBlockingQueue<byte[]> recordQueue;
    private Thread decodeThread;
    private volatile IOException decodeException;
    private boolean iteratorMode = false;
    
    public abstract void decode() throws IOException;
    
    public static TerseDecompresser create(InputStream inputStream, OutputStream outputStream) throws IOException
    {
        DataInputStream input = new DataInputStream(new BufferedInputStream(inputStream));
        TerseHeader header_rv = TerseHeader.CheckHeader(input);
        
        if (!header_rv.SpackFlag) {
        	return new NonSpackDecompresser(input, outputStream, header_rv);
        } else {
        	return new SpackDecompresser(input, outputStream, header_rv);
        }
    }

    /**
     * Creates a TerseDecompresser in iterator mode. In this mode the caller retrieves
     * decompressed records one by one via {@link #nextRecord()} instead of having all
     * output written to a stream. Decompression starts immediately on a background thread.
     *
     * @param inputStream the compressed input stream
     * @return a TerseDecompresser ready for record-by-record retrieval
     * @throws IOException if the compressed stream header cannot be read or is invalid
     */
    public static TerseDecompresser create(InputStream inputStream) throws IOException
    {
        DataInputStream input = new DataInputStream(new BufferedInputStream(inputStream));
        TerseHeader header_rv = TerseHeader.CheckHeader(input);

        TerseDecompresser decompresser;
        if (!header_rv.SpackFlag) {
        	decompresser = new NonSpackDecompresser(input, null, header_rv);
        } else {
        	decompresser = new SpackDecompresser(input, null, header_rv);
        }

        decompresser.iteratorMode = true;
        decompresser.recordQueue = new LinkedBlockingQueue<>();

        decompresser.decodeThread = new Thread(() -> {
            try {
                decompresser.decode();
            } catch (IOException e) {
                decompresser.decodeException = e;
            } finally {
                // flush any record that was not yet terminated by a record mark
                try {
                    if (decompresser.record.size() > 0
                            || decompresser.TextFlag && decompresser.VariableFlag) {
                        decompresser.endRecord();
                    }
                } catch (IOException e) {
                    if (decompresser.decodeException == null) {
                        decompresser.decodeException = e;
                    }
                }
                try {
                    decompresser.recordQueue.put(RECORD_EOF);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        // Use a daemon thread so the JVM can exit even if the caller forgets to call close().
        // When close() is called explicitly (e.g. via try-with-resources), it interrupts and
        // joins this thread to ensure orderly cleanup.
        decompresser.decodeThread.setDaemon(true);
        decompresser.decodeThread.start();

        return decompresser;
    }
    
	public TerseDecompresser(InputStream instream, OutputStream outputStream, TerseHeader header)
	{		
		this.RecordLength = header.RecordLength;
		this.HostFlag = header.HostFlag; 
		this.VariableFlag = header.RecfmV;
		this.input = new TerseBlockReader(instream);
		this.stream = outputStream != null
				? new DataOutputStream(new BufferedOutputStream(outputStream))
				: null;
		
		this.record = new ByteArrayOutputStream(RecordLength);
	}
	
    /* Write a new line to the output file*/
    public void endRecord() throws IOException 
    {
    	if (iteratorMode)
    	{
    		byte[] data = record.toByteArray();
    		record.reset();

    		if (VariableFlag && !TextFlag)
    		{
    			// prepend a RDW to the record data
    			int recordlength = data.length + 4;
    			int rdw = recordlength << 16;
    			ByteArrayOutputStream buf = new ByteArrayOutputStream(data.length + 4);
    			DataOutputStream dos = new DataOutputStream(buf);
    			dos.writeInt(rdw);
    			dos.write(data);
    			data = buf.toByteArray();
    		}

    		try {
    			recordQueue.put(data);
    		} catch (InterruptedException e) {
    			Thread.currentThread().interrupt();
    			throw new IOException("Interrupted while queuing record", e);
    		}
    		return;
    	}

    	if (VariableFlag && !TextFlag)
    	{
    		// write a RDW
    		int recordlength = record.size() + 4;
    		int rdw = recordlength << 16;
    		stream.writeInt(rdw);
    	}
    	
    	stream.write(record.toByteArray());
    	record.reset();
    	
    	if (TextFlag)
    	{
    		stream.write(lineseparator);
    	}    		
    }

    /*
     * Write some stuff to the output record
     */

    public void PutChar(int X) throws IOException {
        if (X == 0) {
            if (HostFlag && TextFlag && VariableFlag) {
                endRecord();
            }
        } else {
            if (HostFlag && TextFlag) {
                if (VariableFlag) {
                    if (X == Constants.RECORDMARK) {
                        endRecord();
                    } else {
                    	record.write(Constants.EbcToAsc[X-1]);
                    }
                } else {
                	record.write(Constants.EbcToAsc[X-1]);
                    if (record.size() == RecordLength) {
                        endRecord();
                    }
                }
            } 
            else 
            {
                if (X == Constants.RECORDMARK) 
                {
                	if (VariableFlag)
                	{
                		endRecord();
                	}
                	/* else discard record marks */
                }
                else
                {
                	record.write(X-1);
                }
            }
        }
    }

    /**
     * Returns the next decompressed record as a byte array, or {@code null} when all
     * records have been consumed. Blocks until a record is available or decompression
     * has finished.
     *
     * <p>This method is only valid when the decompresser was created via
     * {@link #create(InputStream)}.
     *
     * @return the next record's bytes, or {@code null} at end of input
     * @throws IOException if decompression encountered an error, or if the calling
     *         thread is interrupted while waiting for the next record
     */
    public byte[] nextRecord() throws IOException {
        if (decodeException != null) {
            throw decodeException;
        }
        try {
            byte[] next = recordQueue.take();
            if (next == RECORD_EOF) {
                // check once more for an exception that was set just before EOF was queued
                if (decodeException != null) {
                    throw decodeException;
                }
                return null;
            }
            return next;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for next record", e);
        }
    }

	@Override
	public void close() throws Exception {
		if (iteratorMode) {
			if (decodeThread != null) {
				// Interrupt the thread so it can exit early if blocked on queue.put().
				// Also close the underlying input stream to unblock any pending I/O read.
				decodeThread.interrupt();
				try {
					this.input.close();
				} catch (Exception ignored) {
					// best-effort; we still need to join the thread below
				}
				try {
					decodeThread.join();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
			return;
		}
		if (record.size() > 0 
				|| TextFlag && VariableFlag)
		{
			endRecord();
		}
		this.stream.close();
		this.input.close();
	}
}
