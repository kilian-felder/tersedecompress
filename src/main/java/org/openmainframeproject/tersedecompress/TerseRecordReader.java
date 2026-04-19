package org.openmainframeproject.tersedecompress;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Provides a record-by-record pull API for decompressing TERSE-compressed files.
 *
 * <p>Usage example:
 * <pre>{@code
 * try (TerseRecordReader reader = TerseRecordReader.open(new FileInputStream("input.terse"), false)) {
 *     byte[] record;
 *     while ((record = reader.nextRecord()) != null) {
 *         // process one record at a time
 *     }
 * }
 * }</pre>
 *
 * <p>Only one decompressed record is held in memory at a time, making this suitable
 * for very large files.
 */
public class TerseRecordReader implements AutoCloseable {

    /** Sentinel value placed in the queue by the decoder thread when it finishes. */
    private static final byte[] SENTINEL = new byte[0];

    /** Milliseconds to wait for the decoder thread to stop during {@link #close()}. */
    private static final long DECODER_SHUTDOWN_TIMEOUT_MS = 5000;

    /** Milliseconds to wait in each poll cycle inside {@link #nextRecord()}. */
    private static final long POLL_TIMEOUT_MS = 10;

    private final LinkedBlockingQueue<byte[]> queue;
    private final Thread decoderThread;
    private final InputStream originalStream;
    private volatile Throwable decoderException;
    private volatile boolean done = false;

    private TerseRecordReader(TerseDecompresser decompresser,
                               LinkedBlockingQueue<byte[]> queue,
                               InputStream originalStream) {
        this.queue = queue;
        this.originalStream = originalStream;
        this.decoderThread = new Thread(() -> {
            try {
                decompresser.decode();
            } catch (Throwable t) {
                // Only store the exception when it is not caused by our own interrupt
                // (which happens during close()).
                if (!Thread.currentThread().isInterrupted()) {
                    decoderException = t;
                }
            } finally {
                try {
                    decompresser.close();
                } catch (Exception e) {
                    // Ignore close errors in the decoder thread; the original stream
                    // is closed by TerseRecordReader.close() regardless.
                }
                try {
                    queue.put(SENTINEL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // If the sentinel cannot be put (we were interrupted by close()),
                    // nextRecord() will detect done==true via the poll timeout loop.
                }
            }
        });
        this.decoderThread.setDaemon(true);
        this.decoderThread.start();
    }

    /**
     * Opens a TERSE-compressed input stream for record-by-record decompression.
     *
     * @param inputStream the compressed input stream; ownership is transferred to
     *                    this reader and the stream will be closed by {@link #close()}
     * @param textMode    {@code true} to apply EBCDIC-to-ASCII conversion (text mode);
     *                    {@code false} for binary mode (no conversion)
     * @return a new {@code TerseRecordReader} ready to deliver records
     * @throws IOException if the stream header is invalid or cannot be read
     */
    public static TerseRecordReader open(InputStream inputStream, boolean textMode) throws IOException {
        DataInputStream dis = new DataInputStream(new BufferedInputStream(inputStream));
        TerseHeader header = TerseHeader.CheckHeader(dis);

        LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>(1);
        TerseDecompresser decompresser;
        if (header.SpackFlag) {
            decompresser = new QueueSpackDecompresser(dis, header, queue);
        } else {
            decompresser = new QueueNonSpackDecompresser(dis, header, queue);
        }
        decompresser.TextFlag = textMode;

        return new TerseRecordReader(decompresser, queue, inputStream);
    }

    /**
     * Returns the next decompressed record as a byte array.
     *
     * <p>This method blocks until a record is available or decompression completes.
     * It may be called repeatedly until it returns {@code null}, which signals that
     * all records have been delivered.
     *
     * @return the next record, or {@code null} when there are no more records
     * @throws IOException if an error occurs during decompression or this reader is
     *                     interrupted while waiting for the next record
     */
    public byte[] nextRecord() throws IOException {
        if (done) {
            return null;
        }
        try {
            byte[] record;
            // Poll with a short timeout so that a close() call (which sets done=true)
            // is detected promptly even if the sentinel was never put into the queue.
            while ((record = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS)) == null) {
                if (done) {
                    return null;
                }
            }
            if (record == SENTINEL) {
                done = true;
                if (decoderException != null) {
                    Throwable ex = decoderException;
                    if (ex instanceof IOException) throw (IOException) ex;
                    throw new IOException("Decompression failed", ex);
                }
                return null;
            }
            return record;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for next record", e);
        }
    }

    /**
     * Closes this reader and releases all associated resources.
     *
     * <p>If the decoder thread is still running it is interrupted and awaited for up
     * to 5 seconds. The underlying input stream is closed unconditionally.
     */
    @Override
    public void close() throws IOException {
        done = true;
        decoderThread.interrupt();
        // Clear the queue so that a decoder thread blocked in queue.put() can be
        // woken up by the interrupt without the put call re-blocking on a full queue.
        queue.clear();
        try {
            decoderThread.join(DECODER_SHUTDOWN_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        originalStream.close();
    }

    // ── Queue-based decompresser variants (package-private inner classes) ─────

    private static class QueueSpackDecompresser extends SpackDecompresser {

        private final LinkedBlockingQueue<byte[]> queue;

        QueueSpackDecompresser(InputStream in, TerseHeader header, LinkedBlockingQueue<byte[]> queue) {
            super(in, OutputStream.nullOutputStream(), header);
            this.queue = queue;
        }

        @Override
        protected void deliverRecord(byte[] data) throws IOException {
            try {
                queue.put(data);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while delivering record", e);
            }
        }
    }

    private static class QueueNonSpackDecompresser extends NonSpackDecompresser {

        private final LinkedBlockingQueue<byte[]> queue;

        QueueNonSpackDecompresser(InputStream in, TerseHeader header, LinkedBlockingQueue<byte[]> queue) {
            super(in, OutputStream.nullOutputStream(), header);
            this.queue = queue;
        }

        @Override
        protected void deliverRecord(byte[] data) throws IOException {
            try {
                queue.put(data);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while delivering record", e);
            }
        }
    }
}
