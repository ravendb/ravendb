using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Sparrow.Json;
using Sparrow.Threading;

namespace Raven.Server.Documents.Sharding.Operations.BulkInsert;

internal class BulkInsertWriter : IAsyncDisposable
{
    protected int MaxSizeInBuffer = 1024 * 1024;

    private readonly CancellationToken _token;
    private readonly DisposeOnceAsync<SingleAttempt> _disposeOnce;

    private Task _asyncWrite;
    private MemoryStream _currentWriter;
    private MemoryStream _backgroundWriter;
    private JsonOperationContext.MemoryBuffer _memoryBuffer;
    private JsonOperationContext.MemoryBuffer _backgroundMemoryBuffer;
    private Stream _requestBodyStream;
    private bool _first;

    internal readonly BulkInsertOperation.BulkInsertStreamExposerContent StreamExposer;

    public BulkInsertWriter(JsonOperationContext ctx, CancellationToken token)
    {
        _token = token;
        StreamExposer = new BulkInsertOperation.BulkInsertStreamExposerContent();

        _currentWriter = new MemoryStream();
        _backgroundWriter = new MemoryStream();
        _asyncWrite = Task.CompletedTask;
        _first = true;

        var returnMemoryBuffer = ctx.GetMemoryBuffer(out _memoryBuffer);
        var returnBackgroundMemoryBuffer = ctx.GetMemoryBuffer(out _backgroundMemoryBuffer);

        _disposeOnce = new DisposeOnceAsync<SingleAttempt>(async () =>
        {
            try
            {
                if (StreamExposer.IsDone)
                    return;

                if (_requestBodyStream != null)
                {
                    _currentWriter.WriteByte((byte)']');
                    _currentWriter.Flush();
                    await _asyncWrite;

                    await WriteToStreamAsync(_currentWriter, _requestBodyStream, _memoryBuffer);
                    await _requestBodyStream.FlushAsync(_token);
                }

                StreamExposer.Done();
            }
            finally
            {
                using (StreamExposer)
                using (returnMemoryBuffer)
                using (returnBackgroundMemoryBuffer)
                {
                    
                }
            }
        });
    }

    public async Task FlushIfNeeded(bool force = false)
    {
        await _currentWriter.FlushAsync(_token);

        if (_currentWriter.Position > MaxSizeInBuffer || _asyncWrite.IsCompleted || force)
        {
            await _asyncWrite.ConfigureAwait(false);

            var tmp = _currentWriter;
            _currentWriter = _backgroundWriter;
            _backgroundWriter = tmp;
            _currentWriter.SetLength(0);

            var tmpBuffer = _memoryBuffer;
            _memoryBuffer = _backgroundMemoryBuffer;
            _backgroundMemoryBuffer = tmpBuffer;

            _asyncWrite = WriteToStreamAsync(tmp, _requestBodyStream, tmpBuffer);
        }
    }

    public async Task WriteAsync(Stream src)
    {
        if (_first == false)
            _currentWriter.WriteByte((byte)',');

        _first = false;

        await WriteToStreamAsync(src, _currentWriter, _memoryBuffer);
    }

    public async Task WriteDirectlyToRequestStreamAsync(Stream src)
    {
        await WriteToStreamAsync(src, _requestBodyStream, _memoryBuffer);
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

    public async Task EnsureStreamAsync(CompressionLevel compression)
    {
        var stream = await StreamExposer.OutputStream;

        if (compression != CompressionLevel.NoCompression)
        {
            stream = new GZipStream(stream, compression, leaveOpen: true);
        }

        _requestBodyStream = stream;

        _currentWriter.WriteByte((byte)'[');
    }

    public async ValueTask DisposeAsync()
    {
        await _disposeOnce.DisposeAsync();
    }

    public void DisposeRequestStream()
    {
        _requestBodyStream?.Dispose();
    }
}
