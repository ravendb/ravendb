using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Collections;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce.OutputToCollection
{
    public class OutputReduceToCollectionActions
    {
        private readonly MapReduceIndex _index;
        private const string ReduceOutputsTreeName = "ReduceOutputsTree";
        internal static Slice PrefixesOfReduceOutputDocumentsToDeleteKey;
        internal static Slice ReduceOutputsIdsToPatternReferenceIdsTree;

        private ConcurrentSet<string> _prefixesOfReduceOutputDocumentsToDelete;
        private readonly string _collectionOfReduceOutputs;
        private readonly long? _reduceOutputVersion;
        private readonly OutputReferencesPattern _patternForOutputReduceToCollectionReferences;

        static OutputReduceToCollectionActions()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "__raven/map-reduce/#prefixes-of-reduce-output-documents-to-delete", ByteStringType.Immutable, out PrefixesOfReduceOutputDocumentsToDeleteKey);
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
            var tree = tx.InnerTransaction.CreateTree(ReduceOutputsTreeName);

            using (var it = tree.MultiRead(PrefixesOfReduceOutputDocumentsToDeleteKey))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    _prefixesOfReduceOutputDocumentsToDelete = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

                    do
                    {
                        _prefixesOfReduceOutputDocumentsToDelete.Add(it.CurrentKey.ToString());

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

        public void AddPrefixesOfDocumentsToDelete(HashSet<string> prefixes)
        {
            if (_prefixesOfReduceOutputDocumentsToDelete == null)
                _prefixesOfReduceOutputDocumentsToDelete = new ConcurrentSet<string>(StringComparer.OrdinalIgnoreCase);

            using (_index._contextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (var tx = context.OpenWriteTransaction())
            {
                var reduceOutputsTree = tx.InnerTransaction.ReadTree(ReduceOutputsTreeName);

                foreach (var prefix in prefixes)
                {
                    if (_prefixesOfReduceOutputDocumentsToDelete.Contains(prefix))
                        continue;

                    reduceOutputsTree.MultiAdd(PrefixesOfReduceOutputDocumentsToDeleteKey, prefix);
                }

                tx.Commit();
            }

            foreach (var prefix in prefixes)
            {
                _prefixesOfReduceOutputDocumentsToDelete.Add(prefix);
            }
        }

        public ConcurrentSet<string> GetPrefixesOfDocumentsToDelete()
        {
            return _prefixesOfReduceOutputDocumentsToDelete;
        }

        public bool HasDocumentsToDelete()
        {
            return _prefixesOfReduceOutputDocumentsToDelete != null && _prefixesOfReduceOutputDocumentsToDelete.Count > 0;
        }

        public bool HasDocumentsToDelete(TransactionOperationContext indexContext)
        {
            var reduceOutputsTree = indexContext.Transaction.InnerTransaction.ReadTree(ReduceOutputsTreeName);

            if (reduceOutputsTree != null)
            {
                using (var it = reduceOutputsTree.MultiRead(PrefixesOfReduceOutputDocumentsToDeleteKey))
                {
                    if (it.Seek(Slices.BeforeAllKeys))
                    {
                        return true;
                    }
                }
            }

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
                    var command = new DeleteReduceOutputDocumentsCommand(database, prefix, deleteBatchSize);

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
                        prefixesToDelete.Add(prefix);

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
            var reduceOutputsTree = indexContext.Transaction.InnerTransaction.ReadTree(ReduceOutputsTreeName);

            reduceOutputsTree.MultiDelete(PrefixesOfReduceOutputDocumentsToDeleteKey, prefix);

            _prefixesOfReduceOutputDocumentsToDelete.TryRemove(prefix);
        }
    }
}
