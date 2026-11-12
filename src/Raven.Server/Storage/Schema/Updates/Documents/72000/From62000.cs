using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Raven.Client.Documents.Attachments;
using Raven.Server.Documents;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Logging;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Impl;
using Voron.Util;

namespace Raven.Server.Storage.Schema.Updates.Documents
{
    public class From62000 : ISchemaUpdate
    {
        public int From => 62_000;
        public int To => 72_000;
        public SchemaUpgrader.StorageType StorageType => SchemaUpgrader.StorageType.Documents;

        private static readonly string AttachmentsMetadata = "AttachmentsMetadata";
        private static readonly string Attachments = "Attachments";
        private static readonly string AttachmentsEtag = "AttachmentsEtag";
        private static readonly string AttachmentsHash = "AttachmentsHash";
        private static readonly string AttachmentsFlagAndHash = "AttachmentsFlagAndHash";
        private static readonly int _oldSchemaSize = 7;

        internal static int NumberOfAttachmentsToMigrateInSingleTransaction = PlatformDetails.Is32Bits ? 2048 : 32768;

        // Progress is reported at most once per percent and interval, because every database load timeout exception carries the whole init log.
        internal static TimeSpan MinimumProgressReportInterval = TimeSpan.FromSeconds(10);

        internal static TestingStuff ForTestingPurposes;

        internal static TestingStuff ForTestingPurposesOnly() => ForTestingPurposes ??= new TestingStuff();

        internal sealed class TestingStuff
        {
            // Called with the number of batches committed so far, in both passes.
            internal Action<int> AfterBatchCommit;
            internal int CommittedBatches;
        }

        private static void OnBatchCommitted()
        {
            TestingStuff testingStuff = ForTestingPurposes;
            testingStuff?.AfterBatchCommit?.Invoke(++testingStuff.CommittedBatches);
        }
        
        private static readonly Slice AttachmentsEtagSlice;
        private static readonly Slice AttachmentsHashSlice;
        private static readonly Slice AttachmentsFlagAndHashSlice;

        static From62000()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, AttachmentsEtag, ByteStringType.Immutable, out AttachmentsEtagSlice);
                Slice.From(ctx, AttachmentsHash, ByteStringType.Immutable, out AttachmentsHashSlice);
                Slice.From(ctx, AttachmentsFlagAndHash, ByteStringType.Immutable, out AttachmentsFlagAndHashSlice);
            }
        }

        /*
         *  We are only updating the attachments table, so we can add missing fields in the table, and correctly populate the dynamic index.
         *  The documents table is not updated, because we are not adding RemoteParameters to document if the parameters are null, this saves us document table space and schema update.
         */
        public bool Update(UpdateStep step)
        {
            var isSharded = step.DocumentsStorage is ShardedDocumentsStorage;
            step.WriteTx.CreateTree(AttachmentsFlagAndHashSlice, isIndexTree: true);
            
            TableSchema attachmentsSchemaBase = new TableSchema();
            
            attachmentsSchemaBase.DefineKey(new TableSchema.IndexDef
            {
                StartIndex = (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType,
                Count = 1
            });
            attachmentsSchemaBase.DefineFixedSizeIndex(new TableSchema.FixedSizeKeyIndexDef
            {
                StartIndex = (int)AttachmentsTable.Etag,
                Name = AttachmentsEtagSlice
            });
            attachmentsSchemaBase.DefineIndex(new TableSchema.IndexDef
            {
                StartIndex = (int)AttachmentsTable.Hash,
                Count = 1,
                Name = AttachmentsHashSlice
            });

            var dynamicIndex = new TableSchema.DynamicKeyIndexDef
            {
                GenerateKey = GenerateFlagAndHashForAttachments,
                IsGlobal = true,
                Name = AttachmentsFlagAndHashSlice,
                SupportDuplicateKeys = true
            };

            attachmentsSchemaBase.DefineIndex(dynamicIndex);

            if (isSharded)
            {
                attachmentsSchemaBase.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = AttachmentsStorage.GenerateBucketAndHashForAttachments,
                    IsGlobal = true,
                    Name = Raven.Server.Documents.Schemas.Attachments.AttachmentsBucketAndHashSlice,
                    SupportDuplicateKeys = true
                });

                attachmentsSchemaBase.DefineIndex(new TableSchema.DynamicKeyIndexDef
                {
                    GenerateKey = AttachmentsStorage.GenerateBucketAndEtagIndexKeyForAttachments,
                    OnEntryChanged = AttachmentsStorage.UpdateBucketStatsForAttachments,
                    IsGlobal = true,
                    Name = Raven.Server.Documents.Schemas.Attachments.AttachmentsBucketAndEtagSlice
                });
            }

            PopulateAttachmentsFlagAndHashIndex(step, attachmentsSchemaBase, dynamicIndex);
            UpdateSchemaInternal(step, attachmentsSchemaBase, isSharded);
            // This code misses the schemaBase.SerializeSchemaIntoTableTree call.
            // It works for runtime operations, but not for features that use offline database files,
            // which use the serialized schema from disk to perform their operations.
            // The schema was repaired in From72001.cs

            return true;
        }

        // Every attachment must be in the new index before UpdateSchemaInternal rewrites any record: Table.Set can move neighbouring records,
        // and Table.OnDataMoved throws for a moved record that is missing from an index. During the upgrade the key of every attachment
        // is (RemoteAttachmentFlags.None, hash), so the index is built from the AttachmentsHash index without reading records or streams.
        private void PopulateAttachmentsFlagAndHashIndex(UpdateStep step, TableSchema attachmentsSchemaBase, TableSchema.DynamicKeyIndexDef dynamicIndex)
        {
            Table table = step.ReadTx.OpenTable(attachmentsSchemaBase, AttachmentsMetadata);
            if (table == null)
                return;

            UpgradeProgress progress = new(step, $"Schema upgrade from version #{From} to version #{To}: pass 1 of 2", table.NumberOfEntries);
            byte[] lastHash = null;
            long lastId = 0;
            long migrated = 0;

            while (true)
            {
                Table readTable = step.ReadTx.OpenTable(attachmentsSchemaBase, AttachmentsMetadata);
                Table writeTable = step.WriteTx.OpenTable(attachmentsSchemaBase, AttachmentsMetadata);
                Tree hashIndexTree = readTable.GetTree(attachmentsSchemaBase.Indexes[AttachmentsHashSlice]);
                Tree flagAndHashIndexTree = writeTable.GetTree(dynamicIndex);
                long processed = 0;
                bool batchFull = false;

                using (GetBatchStartKey(step.ReadTx.Allocator, lastHash, Slices.BeforeAllKeys, out Slice startHash))
                using (TreeIterator hashIt = hashIndexTree.Iterate(prefetch: true))
                {
                    if (hashIt.Seek(startHash))
                    {
                        do
                        {
                            bool isResumedHash = lastHash != null && hashIt.CurrentKey.AsReadOnlySpan().SequenceEqual(lastHash);
                            FixedSizeTree ids = readTable.GetFixedSizeTree(hashIndexTree, hashIt.CurrentKey, 0, isGlobal: false);

                            using (GenerateFlagAndHashKey(step.WriteTx.Allocator, (int)RemoteAttachmentFlags.None, hashIt.CurrentKey, out Slice flagAndHash))
                            using (FixedSizeTree.IFixedSizeIterator idIt = ids.Iterate())
                            {
                                // Resuming inside a hash continues after the last id of the previous batch. Re-adding an id is a no-op.
                                if (idIt.Seek(isResumedHash ? lastId + 1 : long.MinValue) == false)
                                    continue;

                                do
                                {
                                    writeTable.AddValueToDynamicIndex(idIt.CurrentKey, dynamicIndex, flagAndHashIndexTree, flagAndHash, TreeNodeFlags.Data);

                                    if (++processed >= NumberOfAttachmentsToMigrateInSingleTransaction)
                                    {
                                        lastHash = hashIt.CurrentKey.AsReadOnlySpan().ToArray();
                                        lastId = idIt.CurrentKey;
                                        batchFull = true;
                                        break;
                                    }
                                } while (idIt.MoveNext());
                            }
                        } while (batchFull == false && hashIt.MoveNext());
                    }
                }

                step.Commit(null);
                step.RenewTransactions();
                OnBatchCommitted();
                migrated += processed;

                if (batchFull == false)
                {
                    progress.Completed(migrated);
                    return;
                }

                progress.BatchCommitted(migrated);
            }
        }

        private static IDisposable GetBatchStartKey(ByteStringContext allocator, byte[] lastKey, Slice firstBatchKey, out Slice startKey)
        {
            if (lastKey == null)
            {
                startKey = firstBatchKey;
                return null;
            }

            return Slice.From(allocator, lastKey, ByteStringType.Immutable, out startKey);
        }

        // Each batch continues after the primary key of the last migrated record. Table.Set keeps the primary key,
        // so the key is still in the next read transaction.
        private void UpdateSchemaInternal(UpdateStep step, TableSchema attachmentsSchemaBase, bool isSharded)
        {
            byte[] lastKey = null;
            var migrated = 0L;
            var done = false;

            UpgradeProgress progress = new(step, $"Schema upgrade from version #{From} to version #{To}: pass 2 of 2",
                step.ReadTx.OpenTable(attachmentsSchemaBase, AttachmentsMetadata)?.NumberOfEntries ?? 0);

            while (done == false)
            {
                using (step.AllocateDocumentsOperationContext(out DocumentsOperationContext context))
                {
                    if (isSharded)
                    {
                        step.WriteTx.Owner = context;
                        step.WriteTx.OnBeforeCommit += ((ShardedDocumentsStorage)step.DocumentsStorage).OnBeforeCommit;
                        step.WriteTx.LowLevelTransaction.OnRollBack += ((ShardedDocumentsStorage)step.DocumentsStorage).OnFailure;
                    }

                    context.TransactionMarkerOffset = (short)step.WriteTx.LowLevelTransaction.Id;
                    var commit = false;
                    var readTable = step.ReadTx.OpenTable(attachmentsSchemaBase, AttachmentsMetadata);
                    if (readTable != null)
                    {
                        var writeTable = step.WriteTx.OpenTable(attachmentsSchemaBase, AttachmentsMetadata);

                        var streamsTree = step.ReadTx.ReadTree(Attachments);
                        var processed = 0L;

                        using (GetBatchStartKey(context.Allocator, lastKey, Slices.Empty, out Slice startAfter))
                        {
                            foreach (var (key, read) in readTable.SeekByPrimaryKeyPrefix(Slices.BeforeAllKeys, startAfter, skip: 0))
                            {
                                using (TableValueReaderUtil.CloneTableValueReader(context, read))
                                {
                                    Attachment attachmentOld = TableValueToAttachmentOld(context, streamsTree, ref read.Reader, out var scope);

                                    using (scope)
                                    using (AttachmentOldToTableValue(context, writeTable, attachmentOld, out var tvb, out _))
                                    {
                                        writeTable.Set(tvb, forceUpdate: true);
                                    }
                                }

                                if (++processed >= NumberOfAttachmentsToMigrateInSingleTransaction)
                                {
                                    lastKey = key.AsReadOnlySpan().ToArray();
                                    commit = true;
                                    break;
                                }
                            }
                        }

                        migrated += processed;

                        if (commit)
                        {
                            step.Commit(context);
                            step.RenewTransactions();
                            OnBatchCommitted();
                            progress.BatchCommitted(migrated);
                            continue;
                        }
                    }

                    done = true;

                    if (isSharded)
                    {
                        // for sharded we need to manually commit here, since it is using the context in the tx.OnBeforeCommit event
                        step.Commit(context);
                        step.RenewTransactions();
                        step.WriteTx.Owner = null;
                        step.WriteTx.OnBeforeCommit -= ((ShardedDocumentsStorage)step.DocumentsStorage).OnBeforeCommit;
                        step.WriteTx.LowLevelTransaction.OnRollBack -= ((ShardedDocumentsStorage)step.DocumentsStorage).OnFailure;
                    }
                }
            }

            progress.Completed(migrated);
        }

        private sealed class UpgradeProgress
        {
            private readonly UpdateStep _step;
            private readonly string _prefix;
            private readonly long _total;
            private readonly Stopwatch _sinceLastReport = Stopwatch.StartNew();
            private int _lastReportedPercent;
            private long _lastReported = -1;

            public UpgradeProgress(UpdateStep step, string prefix, long total)
            {
                _step = step;
                _prefix = prefix;
                _total = total;
            }

            public void BatchCommitted(long migrated)
            {
                int percent = Percent(migrated);
                if (percent <= _lastReportedPercent || _sinceLastReport.Elapsed < MinimumProgressReportInterval)
                    return;

                _lastReportedPercent = percent;
                Report(migrated, percent);
            }

            public void Completed(long migrated)
            {
                if (migrated != _lastReported)
                    Report(migrated, Percent(migrated));
            }

            private int Percent(long migrated) => _total == 0 ? 100 : (int)(migrated * 100 / _total);

            private void Report(long migrated, int percent)
            {
                _lastReported = migrated;
                _sinceLastReport.Restart();
                _step.DocumentsStorage.DocumentDatabase.AddToInitLog?.Invoke(LogLevel.Debug,
                    $"{_prefix}, migrated {migrated:#,#;;0} of {_total:#,#;;0} attachments ({percent}%)");
            }
        }

        private unsafe IDisposable AttachmentOldToTableValue(DocumentsOperationContext context, Table writeTable, Attachment attachment, out TableValueBuilder tvb, out Slice keySlice)
        {
            var toDispose = new List<IDisposable>();

            toDispose.Add(writeTable.Allocate(out tvb));
            toDispose.Add(Slice.From(context.Allocator, attachment.ChangeVector, out var changeVectorSlice));
            toDispose.Add(Slice.From(context.Allocator, attachment.Key.Buffer, attachment.Key.Size, ByteStringType.Immutable, out keySlice));
            toDispose.Add(DocumentIdWorker.Compatibility.GetLowerIdSliceAndStorageKey(context, attachment.Name, out Slice lowerName1, out Slice namePtr));
            toDispose.Add(DocumentIdWorker.Compatibility.GetLowerIdSliceAndStorageKey(context, attachment.ContentType, out Slice lowerName2, out Slice contentTypePtr));

            tvb.Add(keySlice.Content.Ptr, keySlice.Size);
            tvb.Add(Bits.SwapBytes(attachment.Etag));
            tvb.Add(namePtr);
            tvb.Add(contentTypePtr);
            tvb.Add(attachment.Base64Hash.Content.Ptr, attachment.Base64Hash.Size);
            tvb.Add(attachment.TransactionMarker);
            tvb.Add(changeVectorSlice.Content.Ptr, changeVectorSlice.Size);

            tvb.Add(attachment.Size);
            tvb.Add(Bits.SwapBytes((int)Client.Documents.Attachments.RemoteAttachmentFlags.None));
            tvb.Add(-1L);
            tvb.Add(Slices.Empty);

            return new DisposableAction(() =>
            {
                foreach (var item in toDispose)
                {
                    item.Dispose();
                }
            });
        }

        private static long GetAttachmentStreamLength(Tree tree, Slice hashSlice)
        {
            // GetStreamInfo would read a random data page per attachment; the chunk details in the tree node already hold the size.
            return tree.GetStreamSizeFromChunkDetails(hashSlice) ?? -1;
        }

        private unsafe Attachment TableValueToAttachmentOld(DocumentsOperationContext context, Tree tree, ref TableValueReader tvr, out ByteStringContext<ByteStringMemoryCache>.InternalScope scope)
        {
            var result = new Attachment
            {
                StorageId = tvr.Id,
                Key = DocumentsStorage.TableValueToString(context, (int)AttachmentsTable.LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType, ref tvr),
                Etag = DocumentsStorage.TableValueToEtag((int)AttachmentsTable.Etag, ref tvr),
                ChangeVector = DocumentsStorage.TableValueToChangeVector(context, (int)AttachmentsTable.ChangeVector, ref tvr),
                Name = DocumentsStorage.TableValueToId(context, (int)AttachmentsTable.Name, ref tvr),
                ContentType = DocumentsStorage.TableValueToId(context, (int)AttachmentsTable.ContentType, ref tvr),
                TransactionMarker = *(short*)tvr.Read((int)AttachmentsTable.TransactionMarker, out int _)
            };

            scope = DocumentsStorage.TableValueToSlice(context, (int)AttachmentsTable.Hash, ref tvr, out result.Base64Hash);

            result.Size = GetAttachmentStreamLength(tree, result.Base64Hash);

            return result;
        }

        [StorageIndexEntryKeyGenerator]
        internal static unsafe ByteStringContext.Scope GenerateFlagAndHashForAttachments(Transaction tx, ref TableValueReader tvr, out Slice slice)
        {
            var hashPtr = tvr.Read((int)AttachmentsTable.Hash, out var hashSize);

            int flags = tvr.Count == _oldSchemaSize ? (int)RemoteAttachmentFlags.None : *(int*)tvr.Read((int)AttachmentsTable.Flags, out var size);

            using (Slice.External(tx.Allocator, hashPtr, hashSize, out Slice hash))
                return GenerateFlagAndHashKey(tx.Allocator, flags, hash, out slice);
        }

        private static unsafe ByteStringContext.Scope GenerateFlagAndHashKey(ByteStringContext allocator, int flags, Slice hash, out Slice slice)
        {
            ByteStringContext.Scope scope = allocator.Allocate(sizeof(int) + 1 + hash.Size, out ByteString buffer); // flag + record separator + hash

            Span<byte> span = new Span<byte>(buffer.Ptr, buffer.Length);
            MemoryMarshal.AsBytes(new Span<int>(ref flags)).CopyTo(span);
            buffer.Ptr[sizeof(int)] = SpecialChars.RecordSeparator;
            hash.AsReadOnlySpan().CopyTo(span[(sizeof(int) + 1)..]);

            slice = new Slice(buffer);
            return scope;
        }

        internal enum AttachmentsTable
        {
            /* AND is a record separator.
             * We are you using the record separator in order to avoid loading another files that has the same key prefix,
                e.g. fitz(record-separator)profile.png and fitz0(record-separator)profile.png, without the record separator we would have to load also fitz0 and filter it. */
            LowerDocumentIdAndLowerNameAndTypeAndHashAndContentType = 0,
            Etag = 1,
            Name = 2, // format of lazy string key is detailed in GetLowerIdSliceAndStorageKey
            ContentType = 3, // format of lazy string key is detailed in GetLowerIdSliceAndStorageKey
            Hash = 4, // base64 hash
            TransactionMarker = 5,
            ChangeVector = 6,
            Size = 7,
            Flags = 8,
            RemoteAt = 9,
            Identifier = 10,
        }
    }
}
