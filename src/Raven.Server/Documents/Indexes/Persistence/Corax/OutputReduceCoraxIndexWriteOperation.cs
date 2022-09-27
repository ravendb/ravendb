using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.OutputToCollection;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Exceptions;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Exceptions;
using Voron.Impl;
namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public class OutputReduceCoraxIndexWriteOperation : CoraxIndexWriteOperation
{
    private readonly OutputReduceToCollectionCommandBatcher _outputReduceToCollectionCommandBatcher;
    private readonly TransactionHolder _txHolder;

    public OutputReduceCoraxIndexWriteOperation(MapReduceIndex index, Transaction writeTransaction, CoraxDocumentConverterBase converter, Logger logger, JsonOperationContext indexContext) : base(index,
        writeTransaction, converter, logger)
    {
        Debug.Assert(index.OutputReduceToCollection != null);
        _txHolder = new TransactionHolder(writeTransaction);
        _outputReduceToCollectionCommandBatcher = index.OutputReduceToCollection.CreateCommandBatcher(indexContext, _txHolder);
    }

    public override void Commit(IndexingStatsScope stats)
    {
        var enqueue = CommitOutputReduceToCollection();

        using (_txHolder.AcquireTransaction(out _))
        {
            base.Commit(stats);
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
        catch (ObjectDisposedException e) when (DocumentDatabase.DatabaseShutdown.IsCancellationRequested)
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

    private async Task CommitOutputReduceToCollection()
    {
        foreach (var command in _outputReduceToCollectionCommandBatcher.CreateCommands())
            await DocumentDatabase.TxMerger.Enqueue(command).ConfigureAwait(false);
    }

    public override void UpdateDocument(string keyFieldName, LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
        JsonOperationContext indexContext)
    {
        base.UpdateDocument(keyFieldName, key, sourceDocumentId, document, stats, indexContext);
        
        _outputReduceToCollectionCommandBatcher.DeleteReduce(key);
        _outputReduceToCollectionCommandBatcher.AddReduce(key, document, stats);
    }

    public override void IndexDocument(LazyStringValue key, LazyStringValue sourceDocumentId, object document, IndexingStatsScope stats,
        JsonOperationContext indexContext)
    {
        base.IndexDocument(key, sourceDocumentId, document, stats, indexContext);

        _outputReduceToCollectionCommandBatcher.AddReduce(key, document, stats);
    }

    public override void Delete(LazyStringValue key, IndexingStatsScope stats)
    {
        throw new NotSupportedException("Deleting index entries by id() field isn't supported by map-reduce indexes");
    }

    public override void DeleteReduceResult(LazyStringValue reduceKeyHash, IndexingStatsScope stats)
    {
        base.DeleteReduceResult(reduceKeyHash, stats);

        _outputReduceToCollectionCommandBatcher.DeleteReduce(reduceKeyHash);
    }

    public override void Dispose()
    {
        base.Dispose();

        _outputReduceToCollectionCommandBatcher.Dispose();
    }
}
