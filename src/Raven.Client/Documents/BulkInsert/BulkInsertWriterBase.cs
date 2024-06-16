using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Http;
using Sparrow.Json;
using Sparrow.Threading;
using Sparrow.Utils;

namespace Raven.Client.Documents.BulkInsert;

internal abstract class BulkInsertWriterBase : IAsyncDisposable
{
    protected int MaxSizeInBuffer = 1024 * 1024;

    private readonly CancellationToken _token;
    private readonly DisposeOnceAsync<SingleAttempt> _disposeOnce;

    private Task _asyncWrite;
    private MemoryStream _currentWriteStream;
    private MemoryStream _backgroundWriteStream;
    private JsonOperationContext.MemoryBuffer _memoryBuffer;
    private JsonOperationContext.MemoryBuffer _backgroundMemoryBuffer;
    private bool _isInitialWrite = true;

    private Stream _requestBodyStream;

    internal readonly BulkInsertOperation.BulkInsertStreamExposerContent StreamExposer;

    protected BulkInsertWriterBase(JsonOperationContext ctx, CancellationToken token)
    {
        _token = token;
        StreamExposer = new BulkInsertOperation.BulkInsertStreamExposerContent();

        _currentWriteStream = new MemoryStream();
        _backgroundWriteStream = new MemoryStream();
        _asyncWrite = Task.CompletedTask;

        var returnMemoryBuffer = ctx.GetMemoryBuffer(out _memoryBuffer);
        var returnBackgroundMemoryBuffer = ctx.GetMemoryBuffer(out _backgroundMemoryBuffer);

        _disposeOnce = new DisposeOnceAsync<SingleAttempt>(async () =>
        {
            try
            {
                if (StreamExposer.IsDone)
                    return;

                try
                {
                    if (_requestBodyStream != null)
                    {
                        _currentWriteStream.WriteByte((byte)']');
                        _currentWriteStream.Flush();
                        await _asyncWrite.ConfigureAwait(false);

                        await WriteToStreamAsync(_currentWriteStream, _requestBodyStream, _memoryBuffer).ConfigureAwait(false);
                        await _requestBodyStream.FlushAsync(_token).ConfigureAwait(false);
                    }
                }
                finally
                {
                    StreamExposer.Done();
                }
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

    public void Initialize()
    {
        OnCurrentWriteStreamSet(_currentWriteStream);
    }

    public virtual async Task<bool> FlushIfNeeded(bool force = false)
    {
        if (_currentWriteStream.Position > MaxSizeInBuffer || _asyncWrite.IsCompleted || force)
        {
            await _asyncWrite.ConfigureAwait(false);

            var tmp = _currentWriteStream;
            _currentWriteStream = _backgroundWriteStream;
            _backgroundWriteStream = tmp;
            _currentWriteStream.SetLength(0);

            OnCurrentWriteStreamSet(_currentWriteStream);

            var tmpBuffer = _memoryBuffer;
            _memoryBuffer = _backgroundMemoryBuffer;
            _backgroundMemoryBuffer = tmpBuffer;

            _asyncWrite = WriteToStreamAsync(tmp, _requestBodyStream, tmpBuffer, _isInitialWrite || force);
            _isInitialWrite = false;
            return _isInitialWrite || force;
        }

        return false;
    }

    protected virtual void OnCurrentWriteStreamSet(MemoryStream currentWriteStream)
    {

    }

    protected Task WriteToStreamAsync(Stream src, Stream dst)
    {
        return WriteToStreamAsync(src, dst, _memoryBuffer);
    }

    protected Task WriteToRequestStreamAsync(Stream src)
    {
        return WriteToStreamAsync(src, _requestBodyStream, _memoryBuffer);
    }

    private async Task WriteToStreamAsync(Stream src, Stream dst, JsonOperationContext.MemoryBuffer buffer, bool forceDstFlush = false)
    {
        src.Seek(0, SeekOrigin.Begin);

        while (true)
        {
            int bytesRead = await src.ReadAsync(buffer.Memory.Memory, _token).ConfigureAwait(false);

            if (bytesRead == 0)
                break;

            await dst.WriteAsync(buffer.Memory.Memory.Slice(0, bytesRead), _token).ConfigureAwait(false);

            if (forceDstFlush)
                await dst.FlushAsync(_token).ConfigureAwait(false);
        }
    }

    public async Task EnsureStreamAsync(HttpCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
    {
        var stream = await StreamExposer.OutputStream.ConfigureAwait(false);

        if (compressionLevel != CompressionLevel.NoCompression)
        {
            switch (compressionAlgorithm)
            {
                case HttpCompressionAlgorithm.Gzip:
                    stream = new GZipStream(stream, compressionLevel, leaveOpen: true);
                    break;
#if FEATURE_BROTLI_SUPPORT
                case HttpCompressionAlgorithm.Brotli:
                    stream = new BrotliStream(stream, compressionLevel, leaveOpen: true);
                    break;
#endif
#if FEATURE_ZSTD_SUPPORT
                case HttpCompressionAlgorithm.Zstd:
                    stream = ZstdStream.Compress(stream, compressionLevel, leaveOpen: true);
                    break;
#endif
                default:
                    throw new ArgumentOutOfRangeException(nameof(compressionAlgorithm), compressionAlgorithm, null);
            }
        }

        _requestBodyStream = stream;

        _currentWriteStream.WriteByte((byte)'[');
    }

    public virtual async ValueTask DisposeAsync()
    {
        await _disposeOnce.DisposeAsync().ConfigureAwait(false);
    }

    public void DisposeRequestStream()
    {
        _requestBodyStream?.Dispose();
    }
}
