using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.IO;
using Raven.Server.Documents;
using Sparrow;

namespace Raven.Server.Utils;

/// <summary>
/// Buffers everything written to it so it can be sent later, at whatever pace the receiver reads it.
/// Small payloads stay in memory; once <see cref="MemoryThreshold"/> is exceeded the buffer spills into a temp file,
/// which honors the storage temp path and is encrypted when the database is.
/// The same design as ASP.NET Core's FileBufferingWriteStream, which writes its temp file in plain text.
/// </summary>
internal sealed class TempFileBufferingWriteStream : Stream
{
    internal const int MemoryThreshold = 4 * 1024 * 1024;

    private readonly Func<StreamsTempFile> _createTempFile;

    private RecyclableMemoryStream _memory = RecyclableMemoryStreamFactory.GetRecyclableStream();
    private StreamsTempFile _tempFile;
    private Stream _file;

    public TempFileBufferingWriteStream(Func<StreamsTempFile> createTempFile)
    {
        _createTempFile = createTempFile ?? throw new ArgumentNullException(nameof(createTempFile));
    }

    internal bool SpilledToDisk => _file != null;

    public override bool CanRead => false;

    public override bool CanSeek => false;

    public override bool CanWrite => true;

    public override long Length => _file?.Length ?? _memory.Length;

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        GetWriteTarget(buffer.Length).Write(buffer);
    }

    // the writes complete synchronously (memory or a local temp file), which keeps the JSON writer on its synchronous flush path
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken token)
    {
        Write(buffer.AsSpan(offset, count));
        return Task.CompletedTask;
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken token = default)
    {
        Write(buffer.Span);
        return ValueTask.CompletedTask;
    }

    public override void Flush()
    {
        // nothing to flush until the buffer is drained
    }

    public override Task FlushAsync(CancellationToken token) => Task.CompletedTask;

    /// <summary>
    /// Sends everything written so far to <paramref name="destination"/>
    /// </summary>
    public async Task DrainToAsync(Stream destination, CancellationToken token)
    {
        if (_file == null)
        {
            _memory.Position = 0;
            await _memory.CopyToAsync(destination, token);
            return;
        }

        _file.Flush(); // the encrypted temp file keeps its last block in memory until flushed
        _file.Position = 0;
        await _file.CopyToAsync(destination, token);
    }

    private Stream GetWriteTarget(int count)
    {
        if (_file != null)
            return _file;

        if (_memory.Length + count <= MemoryThreshold)
            return _memory;

        _tempFile = _createTempFile();
        _file = _tempFile.StartNewStream();

        _memory.WriteTo(_file);
        _memory.Dispose();
        _memory = null;

        return _file;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _memory?.Dispose();
            _memory = null;

            using (_tempFile)
            {
                _file?.Dispose();
            }

            _file = null;
            _tempFile = null;
        }

        base.Dispose(disposing);
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();
}
