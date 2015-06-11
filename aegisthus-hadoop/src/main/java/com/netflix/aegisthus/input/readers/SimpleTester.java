package com.netflix.aegisthus.input.readers;

/**
 * User: cgeorge
 * Date: 6/11/15
 * Time: 11:17 AM
 */
public class SimpleTester {
    public static void main(String[] args) {
       int chunkLength = 65536;
        long compressedLength = 441836094935L;
        long unCompressedStartPosition = 509492375275L;
        long unCompressedEndPosition = 509758307747L;

        int startChunk = (int)(unCompressedStartPosition/chunkLength);

        long uncompressedStartOffsetOfStartChunk = ((long)startChunk)*((long)chunkLength);

        int endChunk = (int)(unCompressedEndPosition/chunkLength);

        System.out.println("startChunk = " + startChunk);
        System.out.println("endChunk = " + endChunk);
        System.out.println("uncompressedStartOffsetOfStartChunk = " + uncompressedStartOffsetOfStartChunk);

/*

2015-06-11 12:35:51,789 INFO compression.CompressionMetadata [main]: totalChunkCount=14631119 startChunk=11103739 endChunk=11107798
2015-06-11 12:35:53,760 INFO compression.CompressionMetadata [main]: prev = 137643922443 totalChunkCount=14631119 chunkCount=4060
2015-06-11 12:35:53,780 INFO compression.CompressionMetadata [main]: current length=11676 currentchunk=0 startChunk=11103739 endChunk=11107798 uncompressedStartOffsetOfStartChunk=727694639104 startOffsetChunk=137643922443 compressedLength=181374907821 unCompressedStartPosition=727694687494 unCompressedEndPosition=727960689060 chunkLength=65536
2015-06-11 12:35:53,780 INFO splits.AegCompressedSplit [main]: CompressionMetadata:266001566 datalength=958865004917
2015-06-11 12:35:53,801 INFO splits.AegCompressedSplit [main]: skip bytes=48390 uncompressedStart=727694687494 startOffsetChunk=137643922443 uncompressedEnd=727960689060 compressedSize=181374907821
2015-06-11 12:35:53,810 INFO compression.CompressionInputStream [main]: current length=11676 currentchunk=0 startChunk=11103739 endChunk=11107798
2015-06-11 12:35:53,863 INFO readers.SSTableRecordReader [main]: File: /user/astockton/zoneid/Sandy-ZONE_ID-jb-191-Data.db
2015-06-11 12:35:53,863 INFO readers.SSTableRecordReader [main]: Start: 727694687494
2015-06-11 12:35:53,863 INFO readers.SSTableRecordReader [main]: End: 727960689060
2015-06-11 12:35:53,875 INFO readers.SSTableRecordReader [main]: Creating observable
2015-06-11 12:35:53,900 INFO sstable.SSTableColumnScanner [main]: created observable
2015-06-11 12:35:53,915 INFO readers.SSTableRecordReader [main]: done initializing
2015-06-11 12:35:53,916 INFO sstable.SSTableColumnScanner [pool-4-thread-1]: current pos(727694687494) done (has more)

2015-06-11 12:37:31,956 INFO compression.CompressionInputStream [pool-4-thread-1]: current length=14733 currentchunk=4058 startChunk=11103739 endChunk=11107798
2015-06-11 12:37:31,958 ERROR sstable.SSTableColumnScanner [pool-4-thread-1]: IOException Data:727960409875 hasMore?true


chunkCount=4060
startChunk=11103739
endChunk=11107798
OffsetOfStartChu727694639104
skip bytes=48390
Start:          727694687494
IOException:    727960409875
End :           727960689060
datalength=     958865004917

*/

    }
}
