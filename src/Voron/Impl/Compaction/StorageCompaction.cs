// -----------------------------------------------------------------------
//  <copyright file="Compaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Linq;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Global;
using Voron.Impl.FreeSpace;

namespace Voron.Impl.Compaction
{
    public class CompactionProgress
    {
        public RootObjectType ObjectType;
        public string ObjectName;
        public long ObjectProgress;
        public long ObjectTotal;

        public long GlobalProgress;
        public long GlobalTotal;
    }

    public static unsafe class StorageCompaction
    {
        public const string CannotCompactBecauseOfIncrementalBackup = "Cannot compact a storage that supports incremental backups. The compact operation changes internal data structures on which the incremental backup relays.";

        public static void Execute(StorageEnvironmentOptions srcOptions, 
            StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions compactOptions,
            Action<CompactionProgress> progressReport = null)
        {
            if (srcOptions.IncrementalBackupEnabled)
                throw new InvalidOperationException(CannotCompactBecauseOfIncrementalBackup);

            long minimalCompactedDataFileSize;

            srcOptions.ManualFlushing = true; // prevent from flushing during compaction - we shouldn't touch any source files
            compactOptions.ManualFlushing = true; // let us flush manually during data copy

            using (var existingEnv = new StorageEnvironment(srcOptions))
            using (var compactedEnv = new StorageEnvironment(compactOptions))
            {
                CopyTrees(existingEnv, compactedEnv, progressReport);

                compactedEnv.FlushLogToDataFile();

                compactedEnv.Journal.Applicator.SyncDataFile(compactedEnv.ActiveTransactions.OldestTransaction, 
                    compactedEnv.Journal.Applicator.JournalsToDelete,
                    compactedEnv.Journal.Applicator.LastFlushedJournalObject);
                compactedEnv.Journal.Applicator.DeleteCurrentAlreadyFlushedJournal();

                minimalCompactedDataFileSize = compactedEnv.NextPageNumber * existingEnv.Options.PageSize;
            }

            using (var compactedDataFile = new FileStream(Path.Combine(compactOptions.BasePath, Constants.DatabaseFilename), FileMode.Open, FileAccess.ReadWrite))
            {
                compactedDataFile.SetLength(minimalCompactedDataFileSize);
            }
        }

        private static void CopyTrees(StorageEnvironment existingEnv, StorageEnvironment compactedEnv, Action<CompactionProgress> progressReport = null)
        {
            using (var txr = existingEnv.ReadTransaction())
            using (var rootIterator = txr.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (rootIterator.Seek(Slices.BeforeAllKeys) == false)
                    return;

                var totalTreesCount = txr.LowLevelTransaction.RootObjects.State.NumberOfEntries;
                var copiedTrees = 0L;
                do
                {
                    var treeName = rootIterator.CurrentKey.ToString();
                    var currentKey = rootIterator.CurrentKey.Clone(txr.Allocator);
                    var objectType = txr.GetRootObjectType(currentKey);
                    switch (objectType)
                    {
                        case RootObjectType.None:
                            break;
                        case RootObjectType.VariableSizeTree:
                            copiedTrees = CopyVariableSizeTree(compactedEnv, progressReport, txr, treeName, copiedTrees, totalTreesCount);
                            break;
                        case RootObjectType.EmbeddedFixedSizeTree:
                        case RootObjectType.FixedSizeTree:
                            if (FreeSpaceHandling.IsFreeSpaceTreeName(treeName))
                            {
                                copiedTrees++;// we don't copy the fixed size tree
                                continue;
                            }

                            copiedTrees = CopyFixedSizeTrees(compactedEnv, progressReport, txr, rootIterator, treeName, copiedTrees, totalTreesCount, objectType);
                            break;
                        case RootObjectType.Table:
                            copiedTrees = CopyTableTree(compactedEnv, progressReport, txr, treeName, copiedTrees, totalTreesCount);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Unknown " + objectType);
                    }

                } while (rootIterator.MoveNext());
            }
        }

        private static long CopyFixedSizeTrees(StorageEnvironment compactedEnv, Action<CompactionProgress> progressReport, Transaction txr,
            TreeIterator rootIterator, string treeName, long copiedTrees, long totalTreesCount, RootObjectType type)
        {
            
            var fst = txr.FixedTreeFor(rootIterator.CurrentKey.Clone(txr.Allocator), 0);

            Report(type, treeName, copiedTrees, totalTreesCount, 0, fst.NumberOfEntries, progressReport);

            using (var it = fst.Iterate())
            {
                var copiedEntries = 0L;
                if (it.Seek(Int64.MinValue) == false)
                    return copiedTrees;

                do
                {
                    using (var txw = compactedEnv.WriteTransaction())
                    {
                        var snd = txw.FixedTreeFor(rootIterator.CurrentKey.Clone(txr.Allocator));
                        var transactionSize = 0L;
                        do
                        {
                            Slice val;
                            using (it.Value(out val))
                                snd.Add(it.CurrentKey, val);
                            transactionSize += fst.ValueSize + sizeof (long);
                            copiedEntries++;
                        } while (transactionSize < compactedEnv.Options.MaxScratchBufferSize/2 && it.MoveNext());

                        txw.Commit();
                    }
                    if (fst.NumberOfEntries == copiedEntries)
                        copiedTrees++;

                    Report(type, treeName, copiedTrees, totalTreesCount, copiedEntries, fst.NumberOfEntries, progressReport);
                    compactedEnv.FlushLogToDataFile();
                } while (it.MoveNext());
            }
            return copiedTrees;
        }

        private static unsafe long CopyVariableSizeTree(StorageEnvironment compactedEnv, Action<CompactionProgress> progressReport, Transaction txr,
            string treeName, long copiedTrees, long totalTreesCount)
        {
            var existingTree = txr.ReadTree(treeName);

            Report(RootObjectType.VariableSizeTree, treeName, copiedTrees, totalTreesCount, 0, existingTree.State.NumberOfEntries, progressReport);

            using (var existingTreeIterator = existingTree.Iterate(true))
            {
                if (existingTreeIterator.Seek(Slices.BeforeAllKeys) == false)
                    return copiedTrees;

                using (var txw = compactedEnv.WriteTransaction())
                {
                    txw.CreateTree(treeName);
                    txw.Commit();
                }

                var copiedEntries = 0L;

                do
                {
                    var transactionSize = 0L;

                    using (var txw = compactedEnv.WriteTransaction())
                    {
                        var newTree = txw.ReadTree(treeName);

                        do
                        {
                            var key = existingTreeIterator.CurrentKey;

                            if (existingTreeIterator.Current->Flags == TreeNodeFlags.MultiValuePageRef)
                            {
                                using (var multiTreeIterator = existingTree.MultiRead(key))
                                {
                                    if (multiTreeIterator.Seek(Slices.BeforeAllKeys) == false)
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
                        } while (transactionSize < compactedEnv.Options.MaxScratchBufferSize / 2 && existingTreeIterator.MoveNext());

                        txw.Commit();
                    }

                    if (copiedEntries == existingTree.State.NumberOfEntries)
                        copiedTrees++;

                    Report(RootObjectType.VariableSizeTree, treeName, copiedTrees, totalTreesCount, copiedEntries, existingTree.State.NumberOfEntries, progressReport);

                    compactedEnv.FlushLogToDataFile();
                } while (existingTreeIterator.MoveNext());
            }
            return copiedTrees;
        }

        private static long CopyTableTree(StorageEnvironment compactedEnv, Action<CompactionProgress> progressReport, Transaction txr,
            string treeName, long copiedTrees, long totalTreesCount)
        {
            // Load table
            var tableTree = txr.ReadTree(treeName, RootObjectType.Table);

            // Get the table schema
            var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
            var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
            var schema = TableSchema.ReadFrom(txr.Allocator, schemaPtr, schemaSize);

            // Load table into structure 
            var inputTable = txr.OpenTable(schema, treeName);

            // The next three variables are used to know what our current
            // progress is
            var copiedEntries = 0;

            // It is very important that these slices be allocated in the
            // txr.Allocator, as the intermediate write transactions on
            // the compacted environment will be destroyed between each
            // loop.
            var lastSlice = Slices.BeforeAllKeys;
            long lastFixedIndex = 0L;

            Report(RootObjectType.Table, treeName, copiedTrees, totalTreesCount, copiedEntries, inputTable.NumberOfEntries, progressReport);

            while (copiedEntries < inputTable.NumberOfEntries)
            {
                using (var txw = compactedEnv.WriteTransaction())
                {
                    long transactionSize = 0L;

                    schema.Create(txw, treeName);
                    var outputTable = txw.OpenTable(schema, treeName);

                    if (schema.Key == null)
                    {
                        // There is no primary key, however, there must be at least one index
                        if (schema.Indexes.Count > 0)
                        {
                            // We have a variable size index, use it
                            var index = schema.Indexes.First().Value;

                            foreach (var result in inputTable.SeekForwardFrom(index, lastSlice))
                            {
                                foreach (var entry in result.Results)
                                {
                                    // The table will take care of reconstructing indexes automatically
                                    outputTable.Insert(entry);
                                    copiedEntries++;
                                    transactionSize += entry.Size;
                                }

                                // The transaction has surpassed the allowed
                                // size before a flush
                                if (transactionSize >= compactedEnv.Options.MaxScratchBufferSize/2)
                                {
                                    lastSlice = result.Key;
                                    break;
                                }
                            }
                        }
                        else
                        {
                            // Use a fixed size index
                            var index = schema.FixedSizeIndexes.First().Value;

                            foreach (var entry in inputTable.SeekForwardFrom(index, lastFixedIndex))
                            {

                                // The table will take care of reconstructing indexes automatically
                                outputTable.Insert(entry);
                                copiedEntries++;
                                transactionSize += entry.Size;

                                // The transaction has surpassed the allowed
                                // size before a flush
                                if (transactionSize >= compactedEnv.Options.MaxScratchBufferSize/2)
                                {
                                    lastFixedIndex = index.GetValue(entry);
                                    break;
                                }
                            }
                        }
                    }
                    else
                    {
                        // The table has a primary key, inserts in that order are expected to be faster
                        foreach (var entry in inputTable.SeekByPrimaryKey(lastSlice))
                        {
                            // The table will take care of reconstructing indexes automatically
                            outputTable.Insert(entry);
                            copiedEntries++;
                            transactionSize += entry.Size;

                            // The transaction has surpassed the allowed
                            // size before a flush
                            if (transactionSize >= compactedEnv.Options.MaxScratchBufferSize/2)
                            {
                                schema.Key.GetSlice(txr.Allocator, entry, out lastSlice);
                                break;
                            }
                        }
                    }

                    txw.Commit();
                }

                if (copiedEntries == inputTable.NumberOfEntries)
                    copiedTrees++;

                Report(RootObjectType.Table, treeName, copiedTrees, totalTreesCount, copiedEntries, inputTable.NumberOfEntries, progressReport);

                compactedEnv.FlushLogToDataFile();
            }

            return copiedTrees;
        }

        private static void Report(RootObjectType objectType, string objectName, long globalProgress, long globalTotal, long objectProgress, long objectTotal, Action<CompactionProgress> progressReport)
        {
            if (progressReport == null)
                return;

            progressReport(new CompactionProgress
            {
                ObjectType = objectType,
                ObjectName = objectName,
                ObjectProgress = objectProgress,
                ObjectTotal = objectTotal,
                GlobalProgress = globalProgress,
                GlobalTotal = globalTotal
            });
        }
    }
}