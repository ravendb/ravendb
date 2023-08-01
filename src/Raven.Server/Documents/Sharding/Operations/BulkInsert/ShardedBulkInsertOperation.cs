using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Client.Http;
using Raven.Server.Documents.Sharding.Executors;
using Raven.Server.Documents.Sharding.Handlers.BulkInsert;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Operations.BulkInsert;

internal sealed class ShardedBulkInsertOperation : BulkInsertOperationBase<ShardedBatchCommandData>, IShardedOperation<HttpResponseMessage>, IAsyncDisposable
{
    private readonly bool _skipOverwriteIfUnchanged;
    private readonly ShardedBulkInsertHandler _requestHandler;
    private readonly ShardedDatabaseContext _databaseContext;
    private readonly TransactionOperationContext _context;
    private readonly Dictionary<int, ShardedBulkInsertWriter> _writers;
    private readonly List<IDisposable> _returnContexts = new();
    private readonly Dictionary<int, BulkInsertOperation.BulkInsertCommand> _bulkInsertCommands = new();

    public ShardedBulkInsertOperation(long id, bool skipOverwriteIfUnchanged, ShardedBulkInsertHandler requestHandler, ShardedDatabaseContext databaseContext,
        JsonContextPoolBase<TransactionOperationContext> contextPool, CancellationToken token)
    {
        OperationId = id;
        _skipOverwriteIfUnchanged = skipOverwriteIfUnchanged;
        _requestHandler = requestHandler;
        _databaseContext = databaseContext;

        _returnContexts.Add(contextPool.AllocateOperationContext(out _context));

        _writers = new Dictionary<int, ShardedBulkInsertWriter>(databaseContext.ShardCount);

        foreach (var shardNumber in databaseContext.ShardsTopology.Keys)
        {
            var returnContext = contextPool.AllocateOperationContext(out JsonOperationContext context);

            _returnContexts.Add(returnContext);

            _writers[shardNumber] = new ShardedBulkInsertWriter(context, token);
            _writers[shardNumber].Initialize();
        }
    }

    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.NoCompression;

    public HttpRequest HttpRequest => _requestHandler.HttpContext.Request;
    public HttpResponseMessage Combine(Dictionary<int, ShardExecutionResult<HttpResponseMessage>> results) => null;

    public RavenCommand<HttpResponseMessage> CreateCommandForShard(int shardNumber)
    {
        var bulkInsertCommand = new BulkInsertOperation.BulkInsertCommand(OperationId, _writers[shardNumber].StreamExposer, null, _skipOverwriteIfUnchanged);

        _bulkInsertCommands[shardNumber] = bulkInsertCommand;

        return bulkInsertCommand;
    }

    protected override Task WaitForId()
    {
        return Task.CompletedTask;
    }

    public override async Task StoreAsync(ShardedBatchCommandData command, string id)
    {
        await ExecuteBeforeStore();

        int shardNumber = _databaseContext.GetShardNumberFor(_context, id);

        await _writers[shardNumber].WriteStreamAsync(command.Stream);

        if (command.AttachmentStream.Stream != null)
        {
            await _writers[shardNumber].FlushIfNeeded(force: true);
            await _writers[shardNumber].WriteStreamDirectlyToRequestAsync(command.AttachmentStream.Stream);
        }
        else
        {
            await _writers[shardNumber].FlushIfNeeded();
        }
    }
    

    protected override async Task EnsureStreamAsync()
    {
        if (CompressionLevel != CompressionLevel.NoCompression)
        {
            foreach(var shardNumber in _databaseContext.ShardsTopology.Keys)
            {
                _writers[shardNumber].StreamExposer.Headers.ContentEncoding.Add("gzip");
            }
        }

        BulkInsertExecuteTask = _databaseContext.ShardExecutor.ExecuteParallelForAllAsync(this);

        await Task.WhenAll(_writers.Select(x => x.Value.StreamExposer.OutputStream));

        foreach(var writer in _writers.Values)
        {
            await writer.EnsureStreamAsync(CompressionLevel);
        }
    }

    protected override async Task<BulkInsertAbortedException> GetExceptionFromOperation()
    {
        var getStateOperation = new GetShardedOperationStateOperation(_requestHandler.HttpContext, OperationId);
        var result = await _databaseContext.ShardExecutor.ExecuteParallelForAllAsync(getStateOperation);

        if (result?.Result is OperationMultipleExceptionsResult multipleErrors)
            return new BulkInsertAbortedException(string.Join(',', multipleErrors.Exceptions.Select(x => x.Error)));

        return null;
    }

    public override async Task AbortAsync()
    {
        if (BulkInsertExecuteTask == null)
            return; // nothing was done, nothing to kill

        foreach (var command in _bulkInsertCommands)
        {
            if (command.Value.RequestNodeTag == null)
                continue; // request wasn't created

            try
            {
                await _databaseContext.ShardExecutor.ExecuteSingleShardAsync(new KillOperationCommand(OperationId, command.Value.RequestNodeTag), command.Key);
            }
            catch
            {
                // ignore
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        var disposeOperations = new ExceptionAggregator("Failed to dispose bulk insert operations opened per shard");

        foreach(var writer in _writers.Values)
        {
            await disposeOperations.ExecuteAsync(writer.DisposeAsync());
        }

        if (BulkInsertExecuteTask != null)
        {
            try
            {
                await BulkInsertExecuteTask.ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await ThrowBulkInsertAborted(e, disposeOperations.GetAggregateException()).ConfigureAwait(false);
            }
        }

        var disposeRequests = new ExceptionAggregator("Failed to dispose bulk insert requests opened per shard");

        foreach(var shardNumber in _databaseContext.ShardsTopology.Keys)
        {
            disposeRequests.Execute(() => _writers?[shardNumber].DisposeRequestStream());
        }

        var returnContexts = new ExceptionAggregator("Failed to return bulk insert contexts allocated per shard");

        foreach (IDisposable returnContext in _returnContexts)
        {
            returnContexts.Execute(returnContext);
        }

        disposeOperations.ThrowIfNeeded();
        disposeRequests.ThrowIfNeeded();
        returnContexts.ThrowIfNeeded();
    }
}
