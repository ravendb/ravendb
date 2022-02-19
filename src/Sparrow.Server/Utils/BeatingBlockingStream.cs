using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Threading;

namespace Sparrow.Server.Utils;

public class BeatingBlockingStream : Stream
{
    private readonly BlockingRotatingBuffer _blockingRotatingBuffer;

    public BeatingBlockingStream()
    {
        _blockingRotatingBuffer = new BlockingRotatingBuffer(16);
        _dispose = new DisposeOnce<SingleAttempt>(_blockingRotatingBuffer.Close);
    }

    public override void Flush()
    {
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return _blockingRotatingBuffer.TryTake(buffer, offset, count, cancellationToken);
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotImplementedException();
    }

    public override void SetLength(long value)
    {
        throw new NotImplementedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotImplementedException();
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        return _blockingRotatingBuffer.TryAdd(buffer, offset, count, cancellationToken);
    }

    private readonly DisposeOnce<SingleAttempt> _dispose;

    public override ValueTask DisposeAsync()
    {
        _dispose.Dispose();
        return ValueTask.CompletedTask;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);

        try
        {
            while (true)
            {
                var read = await ReadAsync(buffer, 0, bufferSize, cancellationToken).ConfigureAwait(false);
                    
                if (read == 0)
                    break;
                    
                if (read == -1) // timeout, but we need to keep the other side alive
                {
                    await destination.WriteAsync(Array.Empty<byte>(), 0, 0, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                await destination.WriteAsync(buffer, 0, read, cancellationToken).ConfigureAwait(false);

            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
