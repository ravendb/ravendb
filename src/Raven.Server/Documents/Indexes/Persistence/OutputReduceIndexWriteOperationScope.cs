using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Exceptions;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Server.Exceptions;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.Persistence;

internal class OutputReduceIndexWriteOperationScope<TWriter> where TWriter : IndexWriteOperationBase
{
    private readonly TWriter _writer;
    private readonly OutputReduceToCollectionCommandBatcher _outputReduceToCollectionCommandBatcher;
    private readonly TransactionHolder _txHolder;
    private readonly DocumentDatabase _documentDatabase;
    public bool IsActive;

    public OutputReduceIndexWriteOperationScope(MapReduceIndex index, Transaction writeTransaction, JsonOperationContext indexContext, TWriter writer)
    {
        _writer = writer;
        _txHolder = new TransactionHolder(writeTransaction);
        _outputReduceToCollectionCommandBatcher = index.OutputReduceToCollection.CreateCommandBatcher(indexContext, _txHolder);
        _documentDatabase = index._indexStorage.DocumentDatabase;
        IsActive = false;
    }

    public void Commit(IndexingStatsScope stats)
    {
        using (new IsActiveScope(this))
        {
            var enqueue = CommitOutputReduceToCollection();

            using (_txHolder.AcquireTransaction(out _))
            {
                _writer.Commit(stats);
            }

            try
            {
                using (stats.For(IndexingOperation.Reduce.SaveOutputDocuments))
                {
                    enqueue.GetAwaiter().GetResult();
                }
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (ObjectDisposedException e) when (_documentDatabase.DatabaseShutdown.IsCancellationRequested)
            {
                throw new OperationCanceledException("The operation of writing output reduce documents was cancelled because of database shutdown", e);
            }
            catch (Exception e) when (e.IsOutOfMemory() || e is DiskFullException)
            {
                throw;
            }
            catch (Exception e)
            {
                throw new IndexWriteException("Failed to save output reduce documents to disk", e);
            }
        }
    }

    private async Task CommitOutputReduceToCollection()
    {
        foreach (var command in _outputReduceToCollectionCommandBatcher.CreateCommands())
            await _documentDatabase.TxMerger.Enqueue(command).ConfigureAwait(false);
    }
    
    public void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
    {
        using (new IsActiveScope(this))
        {
            _writer.IndexDocument(key, sourceDocumentId, document, stats, indexContext);

            _outputReduceToCollectionCommandBatcher.AddReduce(key, document, stats);
        }
    }

    public void Delete(LazyStringValue key, IndexingStatsScope stats)
    {
        throw new NotSupportedException("Deleting index entries by id() field isn't supported by map-reduce indexes");
    }

    public void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
    {
        using (new IsActiveScope(this))
        {
            _writer.DeleteReduceResult(reduceKeyHash, stats);

            _outputReduceToCollectionCommandBatcher.DeleteReduce(reduceKeyHash);
        }
    }

    public void UpdateDocument( LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats, JsonOperationContext indexContext)
    {
        using (new IsActiveScope(this))
        {
            _writer.UpdateDocument(key, sourceDocumentId, document, stats, indexContext);
            _outputReduceToCollectionCommandBatcher.DeleteReduce(key);
            _outputReduceToCollectionCommandBatcher.AddReduce(key, document, stats);
        }
    }
    
    public void Dispose()
    {
        _outputReduceToCollectionCommandBatcher.Dispose();
    }
    
    private readonly struct IsActiveScope : IDisposable
    {
        private readonly OutputReduceIndexWriteOperationScope<TWriter> _scope;

        public IsActiveScope(OutputReduceIndexWriteOperationScope<TWriter> scope)
        {
            _scope = scope;
            _scope.IsActive = true;
        }
        
        public void Dispose()
        {
            _scope.IsActive = false;
        }
    }
}
