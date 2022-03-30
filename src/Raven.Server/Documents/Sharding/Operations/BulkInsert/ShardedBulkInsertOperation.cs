using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions.Documents.BulkInsert;
using Raven.Client.Http;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Threading;

namespace Raven.Server.Documents.Sharding.Operations.BulkInsert;

internal class ShardedBulkInsertOperation : BulkInsertOperationBase<Stream>, IShardedOperation<HttpResponseMessage>, IAsyncDisposable
{
    // TODO arek - logging

    private readonly bool _skipOverwriteIfUnchanged;
    private readonly ShardedDatabaseContext _databaseContext;
    private readonly TransactionOperationContext _context;
    private readonly CancellationToken _token;
    private readonly BulkInsertOperation.BulkInsertStreamExposerContent[] _streamExposerPerShard;
    private readonly MemoryStream[] _currentWriters;
    private readonly MemoryStream[] _backgroundWriters;
    private readonly Task[] _asyncWrites;
    private readonly DisposeOnceAsync<SingleAttempt>[] _disposeOnce;

    private Stream[] _requestBodyStreamPerShard;
    private readonly bool[] _first;


    public ShardedBulkInsertOperation(long id, bool skipOverwriteIfUnchanged, ShardedDatabaseContext databaseContext, TransactionOperationContext context, CancellationToken token)
    {
        OperationId = id;
        _skipOverwriteIfUnchanged = skipOverwriteIfUnchanged;
        _databaseContext = databaseContext;
        _context = context;
        _token = token;

        _streamExposerPerShard = new BulkInsertOperation.BulkInsertStreamExposerContent[databaseContext.ShardCount];
        _currentWriters = new MemoryStream[databaseContext.ShardCount];
        _backgroundWriters = new MemoryStream[databaseContext.ShardCount];
        _asyncWrites = new Task[databaseContext.ShardCount];
        _first = new bool[databaseContext.ShardCount];

        for (int i = 0; i < databaseContext.ShardCount; i++)
        {
            _streamExposerPerShard[i] = new BulkInsertOperation.BulkInsertStreamExposerContent();
            _currentWriters[i] = new MemoryStream();
            _backgroundWriters[i] = new MemoryStream();
            _asyncWrites[i] = Task.CompletedTask;
            _first[i] = true;
        }

        _disposeOnce = new DisposeOnceAsync<SingleAttempt>[databaseContext.ShardCount];

        for (int i = 0; i < databaseContext.ShardCount; i++)
        {
            var shardNumber = i;

            _disposeOnce[shardNumber] = new DisposeOnceAsync<SingleAttempt>(async () =>
            {
                try
                {
                    if (_streamExposerPerShard[shardNumber].IsDone)
                        return;

                    if (_requestBodyStreamPerShard?[shardNumber] != null)
                    {
                        _currentWriters[shardNumber].WriteByte((byte)']');
                        _currentWriters[shardNumber].Flush();
                        await _asyncWrites[shardNumber];

                        _currentWriters[shardNumber].TryGetBuffer(out var buffer);
                        await _requestBodyStreamPerShard[shardNumber].WriteAsync(buffer.Array, buffer.Offset, buffer.Count, _token);
                        await _requestBodyStreamPerShard[shardNumber].FlushAsync(_token);
                    }

                    _streamExposerPerShard[shardNumber].Done();
                }
                finally
                {
                    _streamExposerPerShard[shardNumber]?.Dispose();
                }
            });
        }
    }

    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.NoCompression;

    public HttpResponseMessage Combine(Memory<HttpResponseMessage> results) => null;

    public RavenCommand<HttpResponseMessage> CreateCommandForShard(int shardNumber)
    {
        return new BulkInsertOperation.BulkInsertCommand(OperationId, _streamExposerPerShard[shardNumber], null, _skipOverwriteIfUnchanged);
    }

    protected override bool HasStream => _requestBodyStreamPerShard != null;

    protected override Task WaitForId()
    {
        return Task.CompletedTask;
    }

    public override async Task StoreAsync(Stream command, string id)
    {
        await ExecuteBeforeStore();

        int shardNumber = _databaseContext.GetShardNumber(_context, id);

        if (_first[shardNumber] == false)
        {
            _currentWriters[shardNumber].WriteByte((byte)',');
        }

        _first[shardNumber] = false;

        await command.CopyToAsync(_currentWriters[shardNumber], _token);

        await FlushIfNeeded(shardNumber);
    }

    private async Task FlushIfNeeded(int shardNumber)
    {
        await _currentWriters[shardNumber].FlushAsync(_token);

        if (_currentWriters[shardNumber].Position > MaxSizeInBuffer ||
            _asyncWrites[shardNumber].IsCompleted)
        {
            await _asyncWrites[shardNumber].ConfigureAwait(false);

            var tmp = _currentWriters[shardNumber];
            _currentWriters[shardNumber] = _backgroundWriters[shardNumber];
            _backgroundWriters[shardNumber] = tmp;
            _currentWriters[shardNumber].SetLength(0);
            tmp.TryGetBuffer(out var buffer);
            _asyncWrites[shardNumber] = _requestBodyStreamPerShard[shardNumber].WriteAsync(buffer.Array, buffer.Offset, buffer.Count, _token);
        }
    }

    protected override async Task EnsureStream()
    {
        if (CompressionLevel != CompressionLevel.NoCompression)
        {
            for (int shardNumber = 0; shardNumber < _databaseContext.ShardCount; shardNumber++)
            {
                _streamExposerPerShard[shardNumber].Headers.ContentEncoding.Add("gzip");
            }
        }

        BulkInsertExecuteTask = _databaseContext.ShardExecutor.ExecuteParallelForAllAsync(this);

        await Task.WhenAll(_streamExposerPerShard.Select(x => x.OutputStream));

        _requestBodyStreamPerShard = new Stream[_streamExposerPerShard.Length];

        for (int shardNumber = 0; shardNumber < _databaseContext.ShardCount; shardNumber++)
        {
            var stream = await _streamExposerPerShard[shardNumber].OutputStream;

            if (CompressionLevel != CompressionLevel.NoCompression)
            {
                stream = new GZipStream(stream, CompressionLevel, leaveOpen: true);
            }

            _requestBodyStreamPerShard[shardNumber] = stream;

            _currentWriters[shardNumber].WriteByte((byte)'[');
        }
    }

    protected override async Task<BulkInsertAbortedException> GetExceptionFromOperation()
    {
        var getStateOperation = new GetShardedOperationStateOperation(OperationId);
        var result = await _databaseContext.ShardExecutor.ExecuteParallelForAllAsync(getStateOperation);

        if (result?.Result is OperationMultipleExceptionsResult multipleErrors)
            return new BulkInsertAbortedException(string.Join(',', multipleErrors.Exceptions.Select(x => x.Error)));

        return null;
    }

    public async ValueTask DisposeAsync()
    {
        var disposeOperations = new ExceptionAggregator("Failed to dispose bulk insert operations opened per shard");

        for (int i = 0; i < _databaseContext.ShardCount; i++)
        {
            await disposeOperations.ExecuteAsync(_disposeOnce[i].DisposeAsync());
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

        for (int i = 0; i < _databaseContext.ShardCount; i++)
        {
            disposeRequests.Execute(_requestBodyStreamPerShard?[i]);
        }

        disposeOperations.ThrowIfNeeded();
        disposeRequests.ThrowIfNeeded();
    }
}
