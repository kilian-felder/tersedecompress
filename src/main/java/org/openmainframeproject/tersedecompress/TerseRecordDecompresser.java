package org.openmainframeproject.tersedecompress;

/**
  Copyright Contributors to the TerseDecompress Project.
  SPDX-License-Identifier: Apache-2.0
**/

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

/**
 * Decompresses a terse-compressed stream and provides record-by-record access
 * via a blocking {@link #nextRecord()} method.
 *
 * <p>The decompression runs in a background thread. Calling {@link #nextRecord()}
 * blocks until a record is available or the end of the compressed data is reached.</p>
 *
 * <p>Supports both text mode (with EBCDIC to ASCII conversion) and binary mode.</p>
 *
 * <p>Usage example:</p>
 * <pre>{@code
 * try (TerseRecordDecompresser rd = new TerseRecordDecompresser(inputStream, true)) {
 *     TerseHeader header = rd.getHeader();
 *     byte[] record;
 *     while ((record = rd.nextRecord()) != null) {
 *         // process record
 *     }
 * }
 * }</pre>
 */
public class TerseRecordDecompresser implements AutoCloseable {

    private static final int PIPE_BUFFER_SIZE = 65536;

    private final TerseHeader header;
    private final boolean textMode;
    private final DataInputStream pipeIn;
    private final Thread decompressThread;
    private volatile IOException decompressException;
    private boolean eof = false;

    /**
     * Creates a new {@code TerseRecordDecompresser} that reads compressed data
     * from the given input stream.
     *
     * @param inputStream the stream containing terse-compressed data
     * @param textMode    {@code true} to enable text mode (EBCDIC to ASCII conversion
     *                    and line-separator–delimited records); {@code false} for binary mode
     * @throws IOException if the header cannot be read or is invalid
     */
    public TerseRecordDecompresser(InputStream inputStream, boolean textMode) throws IOException {
        this.textMode = textMode;

        DataInputStream dis = new DataInputStream(new BufferedInputStream(inputStream));
        this.header = TerseHeader.CheckHeader(dis);

        PipedOutputStream pipeOut = new PipedOutputStream();
        PipedInputStream pipedIn = new PipedInputStream(pipeOut, PIPE_BUFFER_SIZE);
        this.pipeIn = new DataInputStream(new BufferedInputStream(pipedIn));

        TerseDecompresser decomp;
        if (!header.SpackFlag) {
            decomp = new NonSpackDecompresser(dis, pipeOut, header);
        } else {
            decomp = new SpackDecompresser(dis, pipeOut, header);
        }
        decomp.TextFlag = textMode;

        decompressThread = new Thread(new DecompressTask(decomp, pipeOut));
        decompressThread.setDaemon(true);
        decompressThread.start();
    }

    /**
     * Returns the header parsed from the terse-compressed stream.
     *
     * @return the terse file header
     */
    public TerseHeader getHeader() {
        return header;
    }

    /**
     * Returns the next decompressed record, blocking until one is available.
     *
     * <ul>
     *   <li>In text mode, returns the record content without the trailing line separator.</li>
     *   <li>In binary variable-length record (VB) mode, returns the record content without
     *       the record descriptor word (RDW).</li>
     *   <li>In binary fixed-length record (FB) mode, returns exactly {@code RecordLength} bytes.</li>
     * </ul>
     *
     * @return the next record as a byte array, or {@code null} if there are no more records
     * @throws IOException if an I/O error occurs during decompression or reading
     */
    public byte[] nextRecord() throws IOException {
        if (eof) {
            return null;
        }
        byte[] record;
        if (textMode) {
            record = readTextRecord();
        } else if (header.RecfmV) {
            record = readVBRecord();
        } else {
            record = readFBRecord();
        }
        if (record == null) {
            eof = true;
            checkDecompressException();
        }
        return record;
    }

    /**
     * Closes this decompresser and releases all associated resources.
     * If the background decompression thread is still running it is interrupted.
     *
     * @throws IOException if an I/O error occurs while closing
     */
    @Override
    public void close() throws IOException {
        try {
            pipeIn.close();
        } finally {
            decompressThread.interrupt();
            try {
                decompressThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // -- private helpers --

    private byte[] readTextRecord() throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        int b;
        while ((b = pipeIn.read()) != -1) {
            if (b == '\n') {
                return buf.toByteArray();
            }
            if (b != '\r') {
                buf.write(b);
            }
        }
        checkDecompressException();
        return buf.size() > 0 ? buf.toByteArray() : null;
    }

    private byte[] readVBRecord() throws IOException {
        int rdw;
        try {
            rdw = pipeIn.readInt();
        } catch (EOFException e) {
            checkDecompressException();
            return null;
        }
        int length = (rdw >>> 16) & 0xFFFF;
        int dataLength = length - 4;
        if (dataLength < 0) {
            throw new IOException("Invalid RDW: declared length " + length + " is less than 4");
        }
        byte[] record = new byte[dataLength];
        pipeIn.readFully(record);
        return record;
    }

    private byte[] readFBRecord() throws IOException {
        int recordLength = header.RecordLength > 0 ? header.RecordLength : header.RecordLen1;
        if (recordLength <= 0) {
            // No record-length information available: return all remaining bytes as one record.
            byte[] all = pipeIn.readAllBytes();
            checkDecompressException();
            return all.length > 0 ? all : null;
        }
        byte[] record = new byte[recordLength];
        try {
            pipeIn.readFully(record);
            return record;
        } catch (EOFException e) {
            checkDecompressException();
            return null;
        }
    }

    private void checkDecompressException() throws IOException {
        if (decompressException != null) {
            throw decompressException;
        }
    }

    // -- background task --

    private class DecompressTask implements Runnable {
        private final TerseDecompresser decomp;
        private final PipedOutputStream pipeOut;

        DecompressTask(TerseDecompresser decomp, PipedOutputStream pipeOut) {
            this.decomp = decomp;
            this.pipeOut = pipeOut;
        }

        @Override
        public void run() {
            try {
                decomp.decode();
            } catch (IOException e) {
                decompressException = e;
            } catch (Exception e) {
                decompressException = new IOException(e);
            } finally {
                try {
                    decomp.close();
                } catch (Exception e) {
                    if (decompressException == null) {
                        decompressException = e instanceof IOException
                                ? (IOException) e
                                : new IOException(e);
                    }
                }
                try {
                    pipeOut.close();
                } catch (IOException ignored) {
                }
            }
        }
    }
}
