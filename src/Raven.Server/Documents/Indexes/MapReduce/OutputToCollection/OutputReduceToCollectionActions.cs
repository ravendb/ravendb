using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Sparrow.Server.Exceptions;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public sealed class OutputReduceToCollectionActions
    {
        private readonly MapReduceIndex _index;
        private const string PrefixesOfReduceOutputDocumentsToDeleteTree = "PrefixesOfReduceOutputDocumentsToDeleteTree";

        internal static Slice ReduceOutputsIdsToPatternReferenceIdsTree;

        private ConcurrentDictionary<string, string> _prefixesOfReduceOutputDocumentsToDelete;
        private readonly string _collectionOfReduceOutputs;
        private readonly long? _reduceOutputVersion;
        private readonly OutputReferencesPattern _patternForOutputReduceToCollectionReferences;
        private readonly string _referenceDocumentsCollectionName;

        static OutputReduceToCollectionActions()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "ReduceOutputsIdsToPatternReferenceIdsTree", ByteStringType.Immutable, out ReduceOutputsIdsToPatternReferenceIdsTree);
            }
        }

        public string GetCollectionOfReduceOutput()
        {
            return _collectionOfReduceOutputs;
        }

        public string GetPattern()
        {
            return _patternForOutputReduceToCollectionReferences?.Pattern;
        }

        public string GetReferenceDocumentsCollectionName()
        {
            return _referenceDocumentsCollectionName;
        }

        public OutputReduceToCollectionActions(MapReduceIndex index)
        {
            _index = index;

            Debug.Assert(string.IsNullOrEmpty(index.Definition.OutputReduceToCollection) == false);

            _collectionOfReduceOutputs = index.Definition.OutputReduceToCollection;
            _reduceOutputVersion = index.Definition.ReduceOutputIndex;
            _referenceDocumentsCollectionName = index.Definition.PatternReferencesCollectionName;

            if (string.IsNullOrEmpty(index.Definition.PatternForOutputReduceToCollectionReferences) == false)
                _patternForOutputReduceToCollectionReferences = new OutputReferencesPattern(index.DocumentDatabase, index.Definition.PatternForOutputReduceToCollectionReferences, index.Definition.PatternReferencesCollectionName);
        }

        public void Initialize(RavenTransaction tx)
        {
            var tree = tx.InnerTransaction.CreateTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

            if (tx.InnerTransaction.ReadTree(Legacy.LegacyReduceOutputsTreeName) != null)
                Legacy.ConvertLegacyPrefixesToDeleteTree(tx);

            _prefixesOfReduceOutputDocumentsToDelete = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            using (var it = tree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var toDelete = GetPrefixToDeleteAndOriginalPatternFromCurrent(it);

                        if (_prefixesOfReduceOutputDocumentsToDelete.TryAdd(toDelete.Prefix, toDelete.OriginalPattern) == false)
                            throw new InvalidOperationException($"Could not add '{toDelete.Prefix}' prefix to list of items to delete (pattern - '{toDelete.OriginalPattern}')");
                    } while (it.MoveNext());
                }
            }
        }

        public void AddPatternGeneratedIdForReduceOutput(Transaction tx, string reduceResultId, string referenceDocumentId)
        {
            var tree = tx.CreateTree(ReduceOutputsIdsToPatternReferenceIdsTree);

            tree.Add(reduceResultId, referenceDocumentId);
        }

        public string GetPatternGeneratedIdForReduceOutput(Transaction tx, string reduceResultId)
        {
            var tree = tx.CreateTree(ReduceOutputsIdsToPatternReferenceIdsTree);

            var result = tree.Read(reduceResultId);

            // ReSharper disable once UseNullPropagation
            if (result == null)
            {
                // pattern id can be null here if it was invalid for a given reduce result
                return null;
            }

            return result.Reader.ReadString(result.Reader.Length);
        }

        public void DeletePatternGeneratedIdForReduceOutput(Transaction tx, string reduceResultId)
        {
            var tree = tx.CreateTree(ReduceOutputsIdsToPatternReferenceIdsTree);

            tree.Delete(reduceResultId);
        }

        public OutputReduceToCollectionCommandBatcher CreateCommandBatcher(JsonOperationContext indexContext, TransactionHolder writeTxHolder)
        {
            return new OutputReduceToCollectionCommandBatcher(_index.DocumentDatabase, _collectionOfReduceOutputs, _reduceOutputVersion, _patternForOutputReduceToCollectionReferences, _index, indexContext, writeTxHolder);
        }

        public void AddPrefixesOfDocumentsToDelete(Dictionary<string, string> prefixes)
        {
            if (_prefixesOfReduceOutputDocumentsToDelete == null)
                _prefixesOfReduceOutputDocumentsToDelete = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            using (_index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var prefixesToDeleteTree = tx.InnerTransaction.ReadTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

                foreach (var prefix in prefixes)
                {
                    if (_prefixesOfReduceOutputDocumentsToDelete.ContainsKey(prefix.Key))
                        continue;

                    prefixesToDeleteTree.Add(prefix.Key, prefix.Value ?? string.Empty);
                }

                tx.Commit();
            }

            foreach (var prefix in prefixes)
            {
                _prefixesOfReduceOutputDocumentsToDelete.TryAdd(prefix.Key, prefix.Value);
            }
        }

        public ConcurrentDictionary<string, string> GetPrefixesOfDocumentsToDelete()
        {
            return _prefixesOfReduceOutputDocumentsToDelete;
        }

        public bool HasDocumentsToDelete(TransactionOperationContext indexContext)
        {
            var prefixesToDeleteTree = indexContext.Transaction.InnerTransaction.ReadTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

            if (prefixesToDeleteTree != null)
                return prefixesToDeleteTree.State.Header.NumberOfEntries > 0;

            return false;
        }

        public bool DeleteDocuments(IndexingStatsScope stats, TransactionOperationContext indexContext)
        {
            var database = _index.DocumentDatabase;

            const int deleteBatchSize = 1024;

            var prefixesToDelete = new List<string>();

            var deleted = false;

            using (stats.For(IndexingOperation.Reduce.DeleteOutputDocuments))
            {
                var tree = indexContext.Transaction.InnerTransaction.CreateTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

                using (var it = tree.Iterate(false))
                {
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            var toDelete = GetPrefixToDeleteAndOriginalPatternFromCurrent(it);

                            var command = new DeleteReduceOutputDocumentsCommand(database, toDelete.Prefix, toDelete.OriginalPattern, deleteBatchSize);

                            var enqueue = database.TxMerger.Enqueue(command);

                            try
                            {
                                enqueue.GetAwaiter().GetResult();
                            }
                            catch (OperationCanceledException)
                            {
                                throw;
                            }
                            catch (ObjectDisposedException e) when (database.DatabaseShutdown.IsCancellationRequested)
                            {
                                throw new OperationCanceledException("The operation of writing output reduce documents was cancelled because of database shutdown", e);
                            }
                            catch (Exception e) when (e.IsOutOfMemory() || e is DiskFullException)
                            {
                                throw;
                            }
                            catch (Exception e)
                            {
                                throw new IndexWriteException("Failed to delete output reduce documents", e);
                            }

                            if (command.DeleteCount < deleteBatchSize)
                                prefixesToDelete.Add(toDelete.Prefix);

                            if (command.DeleteCount > 0)
                                deleted = true;

                        } while (it.MoveNext());
                    }
                }

                foreach (var prefix in prefixesToDelete)
                {
                    DeletePrefixOfReduceOutputDocumentsToDelete(prefix, indexContext);
                }
            }

            return deleted;
        }

        private void DeletePrefixOfReduceOutputDocumentsToDelete(string prefix, TransactionOperationContext indexContext)
        {
            var reduceOutputsTree = indexContext.Transaction.InnerTransaction.ReadTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

            reduceOutputsTree.Delete(prefix);

            indexContext.Transaction.InnerTransaction.LowLevelTransaction.AfterCommitWhenNewTransactionsPrevented += __ =>
            {
                // ensure that we delete it from in-memory state only after successful commit
                _prefixesOfReduceOutputDocumentsToDelete.TryRemove(prefix, out _);
            };
        }

        private static (string Prefix, string OriginalPattern) GetPrefixToDeleteAndOriginalPatternFromCurrent(TreeIterator it)
        {
            var prefix = it.CurrentKey.ToString();

            var patternValue = it.CreateReaderForCurrent();

            string pattern = null;

            if (patternValue.Length > 0)
                pattern = patternValue.ReadString(patternValue.Length);

            return (prefix, pattern);
        }

        private static class Legacy
        {
            public const string LegacyReduceOutputsTreeName = "ReduceOutputsTree";

            private static readonly Slice LegacyPrefixesOfReduceOutputDocumentsToDeleteKey;

            static Legacy()
            {
                using (StorageEnvironment.GetStaticContext(out var ctx))
                {
                    Slice.From(ctx, "__raven/map-reduce/#prefixes-of-reduce-output-documents-to-delete", ByteStringType.Immutable,
                        out LegacyPrefixesOfReduceOutputDocumentsToDeleteKey);
                }
            }
            public static void ConvertLegacyPrefixesToDeleteTree(RavenTransaction tx)
            {
                var legacyTree = tx.InnerTransaction.ReadTree(LegacyReduceOutputsTreeName);

                var prefixesToDelete = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                using (var it = legacyTree.MultiRead(LegacyPrefixesOfReduceOutputDocumentsToDeleteKey))
                {
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        do
                        {
                            prefixesToDelete.Add(it.CurrentKey.ToString());

                        } while (it.MoveNext());
                    }
                }

                var tree = tx.InnerTransaction.CreateTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

                foreach (string prefix in prefixesToDelete)
                {
                    tree.Add(prefix, string.Empty);
                }

                tx.InnerTransaction.DeleteTree(LegacyReduceOutputsTreeName);
            }
        }
    }
}
