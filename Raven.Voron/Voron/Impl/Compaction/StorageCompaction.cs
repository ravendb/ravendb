// -----------------------------------------------------------------------
//  <copyright file="Compaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Voron.Impl.Paging;
using Voron.Trees;

namespace Voron.Impl.Compaction
{
    public class CompactionProgress
    {
        public string TreeName;

        public long CopiedTrees;
        public long TotalTreeCount;

        public long CopiedTreeRecords;
        public long TotalTreeRecordsCount;
    }

    public unsafe static class StorageCompaction
    {
        public const string CannotCompactBecauseOfIncrementalBackup = "Cannot compact a storage that supports incremental backups. The compact operation changes internal data structures on which the incremental backup relays.";

        public static void Execute(StorageEnvironmentOptions srcOptions, StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions compactOptions, Action<CompactionProgress> progressReport = null)
        {
            if (srcOptions.IncrementalBackupEnabled)
                throw new InvalidOperationException(CannotCompactBecauseOfIncrementalBackup);

            long minimalCompactedDataFileSize;

            srcOptions.ManualFlushing = true; // prevent from flushing during compaction - we shouldn't touch any source files
            compactOptions.ManualFlushing = true; // let us flush manually during data copy

            using(var existingEnv = new StorageEnvironment(srcOptions))
            using (var compactedEnv = new StorageEnvironment(compactOptions))
            {
                CopyTrees(existingEnv, compactedEnv, progressReport);

                compactedEnv.FlushLogToDataFile(allowToFlushOverwrittenPages: true);

                compactedEnv.Journal.Applicator.SyncDataFile();
                compactedEnv.Journal.Applicator.DeleteCurrentAlreadyFlushedJournal();

                minimalCompactedDataFileSize = compactedEnv.NextPageNumber*AbstractPager.PageSize;
            }

            using (var compactedDataFile = new FileStream(Path.Combine(compactOptions.BasePath, Constants.DatabaseFilename), FileMode.Open, FileAccess.ReadWrite))
            {
                compactedDataFile.SetLength(minimalCompactedDataFileSize);
            }
        }

        private static void CopyTrees(StorageEnvironment existingEnv, StorageEnvironment compactedEnv, Action<CompactionProgress> progressReport = null)
        {
            using (var txr = existingEnv.NewTransaction(TransactionFlags.Read))
            using (var rootIterator = txr.Root.Iterate())
            {
                if (rootIterator.Seek(Slice.BeforeAllKeys) == false)
                    return;

                var totalTreesCount = txr.Root.State.EntriesCount;
                var copiedTrees = 0L;

                do
                {
                    var treeName = rootIterator.CurrentKey.ToString();
                    var existingTree = txr.ReadTree(treeName);

                    Report(treeName, copiedTrees, totalTreesCount, 0, existingTree.State.EntriesCount, progressReport);

                    using (var existingTreeIterator = existingTree.Iterate())
                    {
                        if (existingTreeIterator.Seek(Slice.BeforeAllKeys) == false)
                            continue;

                        using (var txw = compactedEnv.NewTransaction(TransactionFlags.ReadWrite))
                        {
                            compactedEnv.CreateTree(txw, treeName);
                            txw.Commit();
                        }

                        var copiedEntries = 0L;

                        do
                        {
                            var transactionSize = 0L;

                            using (var txw = compactedEnv.NewTransaction(TransactionFlags.ReadWrite))
                            {
                                var newTree = txw.ReadTree(treeName);

                                do
                                {
                                    var key = existingTreeIterator.CurrentKey;

                                    if (existingTreeIterator.Current->Flags == NodeFlags.MultiValuePageRef)
                                    {
                                        using (var multiTreeIterator = existingTree.MultiRead(key))
                                        {
                                            if (multiTreeIterator.Seek(Slice.BeforeAllKeys) == false)
                                                continue;

                                            do
                                            {
                                                var multiValue = multiTreeIterator.CurrentKey;
                                                newTree.MultiAdd(key, multiValue);
                                                transactionSize += multiValue.Size;
                                            } while (multiTreeIterator.MoveNext());
                                        }
                                    }
                                    else
                                    {
                                        using (var value = existingTree.Read(key).Reader.AsStream())
                                        {
                                            newTree.Add(key, value);
                                            transactionSize += value.Length;
                                        }
                                    }

                                    copiedEntries++;
                                } while (transactionSize < compactedEnv.Options.MaxLogFileSize/2 && existingTreeIterator.MoveNext());

                                txw.Commit();
                            }

                            if (copiedEntries == existingTree.State.EntriesCount)
                                copiedTrees++;

                            Report(treeName, copiedTrees, totalTreesCount, copiedEntries, existingTree.State.EntriesCount,
                                progressReport);

                            compactedEnv.FlushLogToDataFile();
                        } while (existingTreeIterator.MoveNext());
                    }
                } while (rootIterator.MoveNext());
            }
        }

        private static void Report(string treeName, long copiedTrees, long totalTreeCount, long copiedRecords, long totalTreeRecordsCount, Action<CompactionProgress> progressReport)
        {
            if(progressReport == null)
                return;

            progressReport(new CompactionProgress
            {
                TreeName = treeName,
                CopiedTrees = copiedTrees,
                TotalTreeCount = totalTreeCount,
                CopiedTreeRecords = copiedRecords,
                TotalTreeRecordsCount = totalTreeRecordsCount
            });
        }
    }
}
