/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.aegisthus.io.sstable.compression;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class CompressionMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(CompressionMetadata.class);

    private List<Integer> chunkLengths = Lists.newArrayList();
    private int current;
    private long dataLength;
    private CompressionParameters parameters;
    private long startOffsetChunk = 0;
    private long uncompressedStartOffsetOfStartChunk;
    private int endChunk;
    private int startChunk;

    public CompressionMetadata(InputStream compressionInput, long compressedLength, long unCompressedStartPosition, long unCompressedEndPosition) throws IOException {
        DataInputStream stream = new DataInputStream(compressionInput);

        String compressorName = stream.readUTF();
        int optionCount = stream.readInt();
        Map<String, String> options = Maps.newHashMap();
        for (int i = 0; i < optionCount; ++i) {
            String key = stream.readUTF();
            String value = stream.readUTF();
            options.put(key, value);
        }
        int chunkLength = stream.readInt();

        try {
            parameters = new CompressionParameters(compressorName, chunkLength, options);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Cannot create CompressionParameters for stored parameters", e);
        }

        setDataLength(stream.readLong());

        startChunk = (int)(unCompressedStartPosition/chunkLength);

        uncompressedStartOffsetOfStartChunk = ((long)startChunk)*((long)chunkLength);

        endChunk = (int)(unCompressedEndPosition/chunkLength);

        if (startChunk!=endChunk) {
            chunkLengths = readChunkLengths(stream, compressedLength, startChunk, endChunk);
        }

//        System.out.println("chunkLength = " +chunkLength+" "+ (chunkLength*chunkLengths.size() +" length"+chunkLengths.size()));
//        System.out.println("chunkstartpostition="+chunkLengths.get(1000));
        current = 0;
        LOG.info("current length="+this.currentLength()+" currentchunk="+this.getCurrent()+" startChunk="+this.getStartChunk()+" endChunk="+this.getEndChunk()+" uncompressedStartOffsetOfStartChunk="+uncompressedStartOffsetOfStartChunk
                +" startOffsetChunk="+startOffsetChunk +" compressedLength="+compressedLength+" unCompressedStartPosition="+unCompressedStartPosition+" unCompressedEndPosition="+unCompressedEndPosition + " chunkLength="+chunkLength);

        FileUtils.closeQuietly(stream);
    }

    public int getEndChunk() {
        return endChunk;
    }

    public int getStartChunk() {
        return startChunk;
    }

    public long getUncompressedStartOffsetOfStartChunk() {
        return uncompressedStartOffsetOfStartChunk;
    }

    public CompressionMetadata(InputStream compressionInput, long compressedLength) throws IOException {
        this(compressionInput, compressedLength, 0, 0);
    }

    public long getStartOffsetChunk() {
        return startOffsetChunk;
    }

    public int chunkLength() {
        return parameters.chunkLength();
    }

    public ICompressor compressor() {
        return parameters.sstableCompressor;
    }

    public int currentLength() {
        if (current < chunkLengths.size()) {
            return chunkLengths.get(current);
        } else {
            LOG.info("end of chunk lengths");
        }
        return -1;
    }

    public int getCurrent() {
        return current;
    }

    public long getDataLength() {
        return dataLength;
    }

    public void setDataLength(long dataLength) {
        this.dataLength = dataLength;
    }

    public void incrementChunk() {
        current++;
    }

    private List<Integer> readChunkLengths(DataInput input, long compressedLength, int startChunk, int endChunk) throws IOException {
        int totalChunkCount = input.readInt();
        int chunkCount = totalChunkCount;

        LOG.info("totalChunkCount="+totalChunkCount+" startChunk="+startChunk+" endChunk="+endChunk);
        if (startChunk>0 || (endChunk+1)<totalChunkCount) {
            chunkCount = (endChunk-startChunk)+1;
        }

        if (endChunk+1!=totalChunkCount) {
            chunkCount++;
        }

        List<Integer> lengths = Lists.newArrayList();

        if (startChunk>0) {
            FileUtils.skipBytesFully(input, startChunk*8); // longs are 8 bytes
        }

        long prev = input.readLong();
        startOffsetChunk = prev;

        LOG.info("prev = " + prev + " totalChunkCount=" + totalChunkCount + " chunkCount=" + chunkCount);
        for (int i = 1; i < chunkCount; i++) {
            long cur = input.readLong();
            lengths.add((int) (cur - prev - 4));
            prev = cur;
        }
        if (endChunk+1==totalChunkCount) { //TODO: is endChunk really the totalChunkCount?
            lengths.add((int) (compressedLength - prev - 4));
            LOG.info("ran compressedlength-prev");

        }

        return lengths;
    }
}
