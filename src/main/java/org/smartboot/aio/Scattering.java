package org.smartboot.aio;

import java.nio.ByteBuffer;

/**
 * Scattering read or write
 */
final class Scattering {
    private final ByteBuffer[] buffers;
    private final int offset;
    private final int length;

    public Scattering(ByteBuffer[] buffers, int offset, int length) {
        this.buffers = buffers;
        this.offset = offset;
        this.length = length;
    }

    public ByteBuffer[] getBuffers() {
        return buffers;
    }

    public int getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }
}