// -----------------------------------------------------------------------
//  <copyright file="DetailedStorageReport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using Voron.Data;
using Voron.Data.Tables;
using Voron.Impl.Scratch;

namespace Voron.Debugging
{
    public class SizeReport
    {
        public long DataFileInBytes { get; set; }
        public long JournalsInBytes { get; set; }
        public long TempBuffersInBytes { get; set; }
        public long TempRecyclableJournalsInBytes { get; set; }
    }

    public class StorageReport
    {
        public DataFileReport DataFile { get; set; }
        public List<JournalReport> Journals { get; set; }
        public List<TempBufferReport> TempFiles { get; set; }
        public int CountOfTrees { get; set; }
        public int CountOfTables { get; set; }
    }

    public class DetailedStorageReport
    {
        public InMemoryStorageState InMemoryState { get; set; }
        public DataFileReport DataFile { get; set; }
        public JournalsReport Journals { get; set; }
        public List<TempBufferReport> TempBuffers { get; set; }
        public List<TreeReport> Trees { get; set; }
        public List<TableReport> Tables { get; set; }
        public PreAllocatedBuffersReport PreAllocatedBuffers { get; set; }
        public ScratchBufferPoolInfo ScratchBufferPoolInfo { get; set; }
        public string TotalEncryptionBufferSize { get; set; }
    }

    public class DataFileReport
    {
        public long AllocatedSpaceInBytes { get; set; }
        public long UsedSpaceInBytes { get; set; }
        public long FreeSpaceInBytes { get; set; }
    }

    public class JournalsReport
    {
        public long LastFlushedJournal { get; set; }
        public long TotalWrittenButUnsyncedBytes { get; set; }
        public long LastFlushedTransaction { get; set; }
        public List<JournalReport> Journals { get; set; }
    }

    public class JournalReport
    {
        public long Number { get; set; }
        public long AllocatedSpaceInBytes { get; set; }
        public long Available4Kbs { get; set; }
        public long LastTransaction { get; set; }
        public bool Flushed { get; set; }
    }

    public class TempBufferReport
    {
        public string Name { get; set; }
        public long AllocatedSpaceInBytes { get; set; }
        public TempBufferType Type { get; set; }
    }

    public enum TempBufferType
    {
        Scratch,
        RecyclableJournal
    }

    public class TreeReport
    {
        public RootObjectType Type { get; set; }
        public string Name { get; set; }
        public long PageCount { get; set; }
        public long NumberOfEntries { get; set; }
        public long BranchPages { get; set; }
        public long LeafPages { get; set; }
        public long OverflowPages { get; set; }
        public int Depth { get; set; }
        public double Density { get; set; }
        public MultiValuesReport MultiValues { get; set; }

        public long AllocatedSpaceInBytes { get; set; }

        public long UsedSpaceInBytes { get; set; }

        public StreamsReport Streams { get; set; }
        public Dictionary<int, int> BalanceHistogram { get; internal set; }
    }

    public class MultiValuesReport
    {
        public long NumberOfEntries { get; set; }
        public long PageCount { get; set; }
        public long BranchPages { get; set; }
        public long LeafPages { get; set; }
        public long OverflowPages { get; set; }
    }

    public class PreAllocatedBuffersReport
    {
        public long AllocatedSpaceInBytes { get; set; }
        public long PreAllocatedBuffersSpaceInBytes { get; set; }
        public long NumberOfPreAllocatedPages { get; set; }
        public TreeReport AllocationTree { get; set; }
        public long OriginallyAllocatedSpaceInBytes { get; set; }
    }

    public class StreamsReport
    {
        public List<StreamDetails> Streams { get; set; }

        public long NumberOfStreams { get; set; }

        public long TotalNumberOfAllocatedPages { get; set; }

        public long AllocatedSpaceInBytes { get; set; }
    }

    public class StreamDetails
    {
        public string Name { get; set; }

        public long Length { get; set; }

        public int Version { get; set; }

        public long NumberOfAllocatedPages { get; set; }

        public long AllocatedSpaceInBytes { get; set; }

        public TreeReport ChunksTree { get; set; }
    }
}
