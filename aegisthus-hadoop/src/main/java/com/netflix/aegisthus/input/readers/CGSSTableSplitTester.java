package com.netflix.aegisthus.input.readers;

import com.netflix.aegisthus.io.sstable.IndexDatabaseScanner;
import com.netflix.aegisthus.io.sstable.SSTableColumnScanner;
import com.netflix.aegisthus.io.sstable.compression.CompressionInputStream;
import com.netflix.aegisthus.io.sstable.compression.CompressionMetadata;
import com.netflix.aegisthus.io.writable.AtomWritable;
import com.netflix.aegisthus.util.ObservableToIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import rx.Observable;

import java.io.*;
import java.util.Iterator;

/**
 * User: cgeorge
 * Date: 6/8/15
 * Time: 11:33 AM
 */
public class CGSSTableSplitTester {

    public static void main(String[] args) {
//        String compressionInfo  = "/mnt/maprfs/user/cgeorge/20150604103349/Sandy-ALL-jb-14368-CompressionInfo.db";
        String compressionInfo  = "/Users/cgeorge/Documents/cassandraALLSStableSnapshots/Sandy-ALL-jb-14368-CompressionInfo.db";
        File compressionInfoFile = new File(compressionInfo);
//        String fileName ="/mnt/maprfs/user/cgeorge/20150604103349/Sandy-ALL-jb-14368-Data.db";
        String fileName ="/Users/cgeorge/Documents/cassandraALLSStableSnapshots/Sandy-ALL-jb-14368-Data.db";
        String indexFileName ="/Users/cgeorge/Documents/cassandraALLSStableSnapshots/Sandy-ALL-jb-14368-Index.db";
        File file = new File(fileName);
        try {

            IndexDatabaseScanner indexDatabaseScanner = new IndexDatabaseScanner(new FileInputStream(indexFileName));

//            while (indexDatabaseScanner.hasNext()) {
//                IndexDatabaseScanner.OffsetInfo next = indexDatabaseScanner.next();
//                System.out.println("dataFileOffeset=" + next.getDataFileOffset()+" indexOffset="+next.getIndexFileOffset());
//            }

            /*
            keysize = 17 count=242982 keyCounter=12814
            dataFileOffeset=282899629 indexOffset=422443

            chunkLength = 282918912 number of chunks=4317
             */

            CompressionMetadata compressionMetadata = new CompressionMetadata(new BufferedInputStream(new FileInputStream(compressionInfoFile)), 72956594);
//            LOG.info("CompressionMetadata:"+(getEnd() - getStart())+" datalength="+compressionMetadata.getDataLength());
            CompressionInputStream dis = new CompressionInputStream(new DataInputStream(new FileInputStream(fileName)), compressionMetadata);
//
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
