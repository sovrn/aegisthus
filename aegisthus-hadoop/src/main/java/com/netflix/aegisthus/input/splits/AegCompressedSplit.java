package com.netflix.aegisthus.input.splits;

import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

public class AegCompressedSplit extends AegSplit {
    private static final Logger LOG = LoggerFactory.getLogger(AegCompressedSplit.class);
    private Path compressedPath;
    private Long compressLength;

    public static AegCompressedSplit createAegCompressedSplit(@Nonnull Path path,
            long start,
            long length,
            @Nonnull String[] hosts,
            @Nonnull Path compressedPath,
            @Nonnull long compressLength) {
        AegCompressedSplit split = new AegCompressedSplit();
        split.path = path;
        split.start = start;
        split.end = length + start;
        split.hosts = hosts;
        LOG.info("start: {}, end: {}, path {}", start, split.end, path);
        if (start<0) {
            System.out.println("start:" +start +" end:"+split.end+" path:"+path);
        }
        split.compressedPath = compressedPath;
        split.compressLength = compressLength;

        return split;
    }

//    @Override
//    public long getDataEnd() {
//        if (compressionMetadata == null) {
//            throw new IllegalStateException("getDataEnd was called before getInput");
//        }
//        return compressionMetadata.getDataLength();
//    }


    @Nonnull
    @Override
    public InputStream getInput(@Nonnull Configuration conf, long startPosition) throws IOException {
        FileSystem fs = compressedPath.getFileSystem(conf);
        FSDataInputStream cmIn = fs.open(compressedPath);
        CompressionMetadata compressionMetadata = new CompressionMetadata(new BufferedInputStream(cmIn), compressLength, start, end);
        LOG.info("CompressionMetadata:"+(getEnd() - getStart())+" datalength="+compressionMetadata.getDataLength());


        //TODO: need to figure out how to skip the input stream forward here
        InputStream dis = super.getInput(conf, compressionMetadata.getStartOffsetChunk());//0 doesn't mean anything
        dis = new CompressionInputStream(dis, compressionMetadata);
        if (compressionMetadata.getUncompressedStartOffsetOfStartChunk()<start) {
            long skipBytes = start-compressionMetadata.getUncompressedStartOffsetOfStartChunk();
            LOG.info("skip bytes="+skipBytes+" uncompressedStart="+start+" startOffsetChunk="+compressionMetadata.getStartOffsetChunk()+" uncompressedEnd="+end + "compressedSize="+compressLength);
            IOUtils.skip(dis, skipBytes);
        }
//        end = compressionMetadata.getDataLength(); // the end is the real end
        return dis;
    }

    @Override
    public void readFields(@Nonnull DataInput in) throws IOException {
        super.readFields(in);
        compressedPath = new Path(WritableUtils.readString(in));
        compressLength = WritableUtils.readVLong(in);
    }

    @Override
    public void write(@Nonnull DataOutput out) throws IOException {
        super.write(out);
        WritableUtils.writeString(out, compressedPath.toUri().toString());
        WritableUtils.writeVLong(out, compressLength.longValue());
    }
}
