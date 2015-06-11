package com.netflix.aegisthus.input.readers;

import com.netflix.aegisthus.input.splits.AegCompressedSplit;
import com.netflix.aegisthus.io.sstable.IndexDatabaseScanner;
import com.netflix.aegisthus.io.sstable.SSTableColumnScanner;
import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.util.ObservableToIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import rx.Observable;
import rx.Subscriber;

import java.io.*;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: cgeorge
 * Date: 6/8/15
 * Time: 11:33 AM
 */
public class CGSSTableTester {

    public static void main(String[] args) {
//        String compressionInfo  = "/mnt/maprfs/user/cgeorge/20150604103349/Sandy-ALL-jb-14368-CompressionInfo.db";
        String compressionInfo  = "/Users/cgeorge/Documents/cassandraALLSStableSnapshots/Sandy-ALL-jb-14368-CompressionInfo.db";
        File compressionInfoFile = new File(compressionInfo);
//        String fileName ="/mnt/maprfs/user/cgeorge/20150604103349/Sandy-ALL-jb-14368-Data.db";
        String fileName ="/Users/cgeorge/Documents/cassandraALLSStableSnapshots/Sandy-ALL-jb-14368-Data.db";
        File file = new File(fileName);
        try {


//            long compressedLength = length;

            CompressionMetadata compressionMetadata = new CompressionMetadata(new BufferedInputStream(new FileInputStream(compressionInfoFile)), 72956594, 0, 0);


            long uncompressedDataLength = compressionMetadata.getDataLength();

            long bytesRemaining = uncompressedDataLength;

            IndexDatabaseScanner scanner = null;

            FileInputStream fileInputStream = new FileInputStream(fileName.replaceAll("-Data.db", "-Index.db"));
            BufferedInputStream indexBufferedStream = new BufferedInputStream(fileInputStream);
            scanner = new IndexDatabaseScanner(indexBufferedStream);



//
//            long splitStart = 0;
//            while (splitStart + fuzzySplit < uncompressedDataLength && scanner != null && scanner.hasNext()) {
//                long splitSize = 0;
//                // The scanner returns an offset from the start of the file.
//                while (splitSize < maxSplitSize && scanner.hasNext()) {
//                    IndexDatabaseScanner.OffsetInfo offsetInfo = scanner.next();
//                    splitSize = offsetInfo.getDataFileOffset() - splitStart;
//                }
////                int blkIndex = getBlockIndex(blkLocations, splitStart + (splitSize / 2)); //translate to compressedlength
//                LOG.info("split path: {}:{}:{}", path.getName(), splitStart, splitSize);
////                System.out.println(path.getName() + " splittStart=" + splitStart + " splitSize=" + splitSize);
//                splits.add(AegCompressedSplit.createAegCompressedSplit(path, splitStart, splitSize, blkLocations[blkLocations.length - 1].getHosts(), compressionPath, compressedLength));
//                bytesRemaining -= splitSize;
//                splitStart += splitSize;
//            }
//
//            if (scanner != null) {
//                scanner.close();
//            }
//
//            if (bytesRemaining != 0) {
//                LOG.info("end path: {}:{}:{}", path.getName(), length - bytesRemaining, bytesRemaining);
//                System.out.println("bytes remaining");
//                splits.add(AegCompressedSplit.createAegCompressedSplit(path, uncompressedDataLength - bytesRemaining, bytesRemaining,
//                        blkLocations[blkLocations.length - 1].getHosts(), compressionPath, compressedLength));
//            }
//
//
//
//
//
//
//


            //START HERE
            // 72956594 is the compressed length of -Data.db
//            CompressionMetadata compressionMetadata = new CompressionMetadata(new BufferedInputStream(new FileInputStream(compressionInfoFile)), 72956594);
//            LOG.info("CompressionMetadata:"+(getEnd() - getStart())+" datalength="+compressionMetadata.getDataLength());
            CompressionInputStream dis = new CompressionInputStream(new DataInputStream(new FileInputStream(fileName)), compressionMetadata);
//
            // 282918399 is the uncompressed length of the snappy data file
            final SSTableColumnScanner ssTableColumnScanner = new SSTableColumnScanner(dis, 0, 282918399, 0, Descriptor.fromFilename(fileName).version);
            Observable<AtomWritable> observable = ssTableColumnScanner.observable();
//
            Iterator<AtomWritable> atomWritableIterator = ObservableToIterator.toIterator(observable);
//
            while(atomWritableIterator.hasNext()) {
                AtomWritable next = atomWritableIterator.next();
            }

            System.out.println("DONE");
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
