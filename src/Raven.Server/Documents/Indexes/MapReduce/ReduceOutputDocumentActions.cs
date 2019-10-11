using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Persistence;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Server;
using Voron;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public class ReduceOutputDocumentActions
    {
        private readonly MapReduceIndex _index;
        private const string ReduceOutputsTreeName = "ReduceOutputsTree";
        internal static Slice PrefixesOfReduceOutputDocumentsToDeleteKey;

        private HashSet<string> _prefixesOfReduceOutputDocumentsToDelete;

        static ReduceOutputDocumentActions()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "__raven/map-reduce/#prefixes-of-reduce-output-documents-to-delete", ByteStringType.Immutable, out PrefixesOfReduceOutputDocumentsToDeleteKey);
            }
        }

        public ReduceOutputDocumentActions(MapReduceIndex index)
        {
            _index = index;
        }

        public void Initialize(RavenTransaction tx)
        {
            var tree = tx.InnerTransaction.CreateTree(ReduceOutputsTreeName);

            using (var it = tree.MultiRead(PrefixesOfReduceOutputDocumentsToDeleteKey))
            {
                if (it.Seek(Slices.BeforeAllKeys))
                {
                    _prefixesOfReduceOutputDocumentsToDelete = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                    do
                    {
                        _prefixesOfReduceOutputDocumentsToDelete.Add(it.CurrentKey.ToString());

                    } while (it.MoveNext());
                }
            }
        }

        public void AddPrefixesOfDocumentsToDelete(HashSet<string> prefixes)
        {
            if (_prefixesOfReduceOutputDocumentsToDelete == null)
                _prefixesOfReduceOutputDocumentsToDelete = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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

        public HashSet<string> GetPrefixesOfDocumentsToDelete()
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

            _prefixesOfReduceOutputDocumentsToDelete.Remove(prefix);
        }
    }
}
