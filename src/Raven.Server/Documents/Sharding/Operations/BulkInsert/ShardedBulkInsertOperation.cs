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
using Raven.Server.Documents.Sharding.Handlers.BulkInsert;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Server.Documents.Sharding.Operations.BulkInsert;

internal class ShardedBulkInsertOperation : BulkInsertOperationBase<ShardedBatchCommandData>, IShardedOperation<HttpResponseMessage>, IAsyncDisposable
{
    // TODO arek - logging

    private readonly bool _skipOverwriteIfUnchanged;
    private readonly ShardedDatabaseContext _databaseContext;
    private readonly TransactionOperationContext _context;
    private readonly CancellationToken _token;
    private readonly BulkInsertOperation.BulkInsertStreamExposerContent[] _streamExposers;
    private readonly MemoryStream[] _currentWriters;
    private readonly MemoryStream[] _backgroundWriters;
    private readonly Task[] _asyncWrites;
    private readonly DisposeOnceAsync<SingleAttempt>[] _disposeOnce;
    private readonly JsonOperationContext.MemoryBuffer[] _memoryBuffers;
    private readonly JsonOperationContext.MemoryBuffer[] _backgroundMemoryBuffers;
    private readonly JsonOperationContext.MemoryBuffer.ReturnBuffer[] _returnMemoryBuffers;

    private Stream[] _requestBodyStreams;
    private readonly bool[] _first;

    public ShardedBulkInsertOperation(long id, bool skipOverwriteIfUnchanged, ShardedDatabaseContext databaseContext, TransactionOperationContext context, CancellationToken token)
    {
        OperationId = id;
        _skipOverwriteIfUnchanged = skipOverwriteIfUnchanged;
        _databaseContext = databaseContext;
        _context = context;
        _token = token;

        _streamExposers = new BulkInsertOperation.BulkInsertStreamExposerContent[databaseContext.ShardCount];
        _currentWriters = new MemoryStream[databaseContext.ShardCount];
        _backgroundWriters = new MemoryStream[databaseContext.ShardCount];
        _asyncWrites = new Task[databaseContext.ShardCount];
        _first = new bool[databaseContext.ShardCount];
        _memoryBuffers = new JsonOperationContext.MemoryBuffer[databaseContext.ShardCount];
        _backgroundMemoryBuffers = new JsonOperationContext.MemoryBuffer[databaseContext.ShardCount];
        _returnMemoryBuffers = new JsonOperationContext.MemoryBuffer.ReturnBuffer[databaseContext.ShardCount];

        for (int i = 0; i < databaseContext.ShardCount; i++)
        {
            _streamExposers[i] = new BulkInsertOperation.BulkInsertStreamExposerContent();
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
                    if (_streamExposers[shardNumber].IsDone)
                        return;

                    if (_requestBodyStreams?[shardNumber] != null)
                    {
                        _currentWriters[shardNumber].WriteByte((byte)']');
                        _currentWriters[shardNumber].Flush();
                        await _asyncWrites[shardNumber];

                        await WriteToStreamAsync(_currentWriters[shardNumber], _requestBodyStreams[shardNumber], _memoryBuffers[shardNumber]);
                        await _requestBodyStreams[shardNumber].FlushAsync(_token);
                    }

                    _streamExposers[shardNumber].Done();
                }
                finally
                {
                    _streamExposers[shardNumber]?.Dispose();
                    _returnMemoryBuffers[shardNumber].Dispose();
                }
            });
        }
    }

    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.NoCompression;

    public HttpResponseMessage Combine(Memory<HttpResponseMessage> results) => null;

    public RavenCommand<HttpResponseMessage> CreateCommandForShard(int shardNumber)
    {
        return new BulkInsertOperation.BulkInsertCommand(OperationId, _streamExposers[shardNumber], null, _skipOverwriteIfUnchanged);
    }

    protected override bool HasStream => _requestBodyStreams != null;

    protected override Task WaitForId()
    {
        return Task.CompletedTask;
    }

    public override async Task StoreAsync(ShardedBatchCommandData command, string id)
    {
        await ExecuteBeforeStore();

        int shardNumber = _databaseContext.GetShardNumber(_context, id);

        if (_first[shardNumber] == false) 
            _currentWriters[shardNumber].WriteByte((byte)',');

        _first[shardNumber] = false;

        await WriteToStreamAsync(command.Stream, _currentWriters[shardNumber], _memoryBuffers[shardNumber]);

        if (command.AttachmentStream.Stream != null)
        {
            await FlushIfNeeded(shardNumber, force: true);
            await WriteToStreamAsync(command.AttachmentStream.Stream, _requestBodyStreams[shardNumber], _memoryBuffers[shardNumber]);
        }
        else
        {
            await FlushIfNeeded(shardNumber);
        }
    }

    private async Task WriteToStreamAsync(Stream src, Stream dst, JsonOperationContext.MemoryBuffer buffer)
    {
        src.Seek(0, SeekOrigin.Begin);

        while (true)
        {
            int bytesRead = await src.ReadAsync(buffer.Memory.Memory, _token);

            if (bytesRead == 0)
                break;

            await dst.WriteAsync(buffer.Memory.Memory[..bytesRead], _token);
        }
    }

    private async Task FlushIfNeeded(int shardNumber, bool force = false)
    {
        await _currentWriters[shardNumber].FlushAsync(_token);

        if (_currentWriters[shardNumber].Position > MaxSizeInBuffer ||
            _asyncWrites[shardNumber].IsCompleted || force)
        {
            await _asyncWrites[shardNumber].ConfigureAwait(false);

            var tmp = _currentWriters[shardNumber];
            _currentWriters[shardNumber] = _backgroundWriters[shardNumber];
            _backgroundWriters[shardNumber] = tmp;
            _currentWriters[shardNumber].SetLength(0);

            var tmpBuffer = _memoryBuffers[shardNumber];
            _memoryBuffers[shardNumber] = _backgroundMemoryBuffers[shardNumber];
            _backgroundMemoryBuffers[shardNumber] = tmpBuffer;

            _asyncWrites[shardNumber] = WriteToStreamAsync(tmp, _requestBodyStreams[shardNumber], tmpBuffer); 
        }
    }

    protected override async Task EnsureStream()
    {
        if (CompressionLevel != CompressionLevel.NoCompression)
        {
            for (int shardNumber = 0; shardNumber < _databaseContext.ShardCount; shardNumber++)
            {
                _streamExposers[shardNumber].Headers.ContentEncoding.Add("gzip");
            }
        }

        BulkInsertExecuteTask = _databaseContext.ShardExecutor.ExecuteParallelForAllAsync(this);

        await Task.WhenAll(_streamExposers.Select(x => x.OutputStream));

        _requestBodyStreams = new Stream[_streamExposers.Length];

        for (int shardNumber = 0; shardNumber < _databaseContext.ShardCount; shardNumber++)
        {
            var stream = await _streamExposers[shardNumber].OutputStream;

            if (CompressionLevel != CompressionLevel.NoCompression)
            {
                stream = new GZipStream(stream, CompressionLevel, leaveOpen: true);
            }

            _requestBodyStreams[shardNumber] = stream;

            _currentWriters[shardNumber].WriteByte((byte)'[');

            _returnMemoryBuffers[shardNumber] = _context.GetMemoryBuffer(out var memoryBuffer);
            _memoryBuffers[shardNumber] = memoryBuffer;

            _context.GetMemoryBuffer(out var backgroundMemoryBuffer);
            _backgroundMemoryBuffers[shardNumber] = backgroundMemoryBuffer;
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
            disposeRequests.Execute(_requestBodyStreams?[i]);
        }

        disposeOperations.ThrowIfNeeded();
        disposeRequests.ThrowIfNeeded();
    }
}
