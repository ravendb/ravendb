using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class OutputReduceToCollectionActions
    {
        private readonly MapReduceIndex _index;
        private const string PrefixesOfReduceOutputDocumentsToDeleteTree = "PrefixesOfReduceOutputDocumentsToDeleteTree";

        internal static Slice ReduceOutputsIdsToPatternReferenceIdsTree;

        private ConcurrentDictionary<string, string> _prefixesOfReduceOutputDocumentsToDelete;
        private readonly string _collectionOfReduceOutputs;
        private readonly long? _reduceOutputVersion;
        private readonly OutputReferencesPattern _patternForOutputReduceToCollectionReferences;

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
        
        public OutputReduceToCollectionActions(MapReduceIndex index)
        {
            _index = index;

            Debug.Assert(string.IsNullOrEmpty(index.Definition.OutputReduceToCollection) == false);

            _collectionOfReduceOutputs = index.Definition.OutputReduceToCollection;
            _reduceOutputVersion = index.Definition.ReduceOutputIndex;

            if (string.IsNullOrEmpty(index.Definition.PatternForOutputReduceToCollectionReferences) == false)
                _patternForOutputReduceToCollectionReferences = new OutputReferencesPattern(index.Definition.PatternForOutputReduceToCollectionReferences);
        }

        public void Initialize(RavenTransaction tx)
        {
            if (tx.InnerTransaction.ReadTree(Legacy.LegacyReduceOutputsTreeName) != null)
                Legacy.ConvertLegacyPrefixesToDeleteTree(tx);

            _prefixesOfReduceOutputDocumentsToDelete = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var tree = tx.InnerTransaction.CreateTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

            using (var it = tree.Iterate(false))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    do
                    {
                        var prefix = it.CurrentKey.ToString();

                        var patternValue = it.CreateReaderForCurrent();

                        string pattern = null;

                        if (patternValue.Length > 0)
                            pattern = patternValue.ReadString(patternValue.Length);

                        if (_prefixesOfReduceOutputDocumentsToDelete.TryAdd(prefix, pattern) == false)
                            throw new InvalidOperationException($"Could not add '{prefix}' prefix to list of items to delete (pattern - '{pattern}')");

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

            if (result == null)
                ThrowCouldNotFindPatternGeneratedIdForReduceOutput(reduceResultId);

            return result.Reader.ReadString(result.Reader.Length);
        }

        private void ThrowCouldNotFindPatternGeneratedIdForReduceOutput(string reduceResultId)
        {
            throw new InvalidOperationException($"Could not find pattern generated ID for reduce output: {reduceResultId}");
        }

        public void DeletePatternGeneratedIdForReduceOutput(Transaction tx, string reduceResultId)
        {
            var tree = tx.CreateTree(ReduceOutputsIdsToPatternReferenceIdsTree);

            tree.Delete(reduceResultId);
        }

        public OutputReduceToCollectionCommand CreateCommand(JsonOperationContext indexContext, TransactionHolder writeTxHolder)
        {
            return new OutputReduceToCollectionCommand(_index.DocumentDatabase, _collectionOfReduceOutputs, _reduceOutputVersion, _patternForOutputReduceToCollectionReferences, _index, indexContext, writeTxHolder);
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

        public bool HasDocumentsToDelete()
        {
            return _prefixesOfReduceOutputDocumentsToDelete != null && _prefixesOfReduceOutputDocumentsToDelete.Count > 0;
        }

        public bool HasDocumentsToDelete(TransactionOperationContext indexContext)
        {
            var prefixesToDeleteTree = indexContext.Transaction.InnerTransaction.ReadTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

            if (prefixesToDeleteTree != null)
                return prefixesToDeleteTree.State.NumberOfEntries > 0;

            return false;
        }

        public bool DeleteDocuments(IndexingStatsScope stats, TransactionOperationContext indexContext)
        {
            if (_prefixesOfReduceOutputDocumentsToDelete == null || _prefixesOfReduceOutputDocumentsToDelete.Count == 0)
                return false;

            var database = _index.DocumentDatabase;

            const int deleteBatchSize = 1024;

            var prefixesToDelete = new List<string>();

            var deleted = false;

            using (stats.For(IndexingOperation.Reduce.DeleteOutputDocuments))
            {
                foreach (var prefix in _prefixesOfReduceOutputDocumentsToDelete)
                {
                    var command = new DeleteReduceOutputDocumentsCommand(database, prefix.Key, prefix.Value, deleteBatchSize);

                    var enqueue = database.TxMerger.Enqueue(command);

                    try
                    {
                        enqueue.GetAwaiter().GetResult();
                    }
                    catch (Exception e)
                    {
                        throw new IndexWriteException("Failed to delete output reduce documents", e);
                    }

                    if (command.DeleteCount < deleteBatchSize)
                        prefixesToDelete.Add(prefix.Key);

                    if (command.DeleteCount > 0)
                        deleted = true;
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

            _prefixesOfReduceOutputDocumentsToDelete.TryRemove(prefix, out _);
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

                var tree = tx.InnerTransaction.ReadTree(PrefixesOfReduceOutputDocumentsToDeleteTree);

                foreach (string prefix in prefixesToDelete)
                {
                    tree.Add(prefix, string.Empty);
                }

                tx.InnerTransaction.DeleteTree(LegacyReduceOutputsTreeName);
            }
        }
    }
}
