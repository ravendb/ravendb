using System;
using System.Threading;
using System.Threading.Tasks;

namespace Sparrow.Server.Utils;

public class BlockingRotatingBuffer
{
    private readonly int _size;
    private readonly BufferHolder[] Buffers;

    public int NextReadPosition;
    private readonly SemaphoreSlim _waitForFreeBuffer;
    private TaskCompletionSource<bool> _waitNewEntry = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    private CancellationTokenSource _closeTokenSource = new CancellationTokenSource();
    public BlockingRotatingBuffer(int size)
    {
        _size = size;
        Buffers = new BufferHolder[size];
        for (int i = 0; i < size; i++)
        {
            Buffers[i] = new BufferHolder();
        }
        _waitForFreeBuffer = new SemaphoreSlim(_size);
    }

    public async Task TryAdd(byte[] buffer, int offset, int count, CancellationToken token)
    {
        if (buffer.Length - offset < count || count <= 0)
            throw new ArgumentException();

        var waitingToken = CancellationTokenSource.CreateLinkedTokenSource(_closeTokenSource.Token, token);
        try
        {
            await _waitForFreeBuffer.WaitAsync(waitingToken.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        var current = NextReadPosition;

        while (true)
        {
            if (Interlocked.CompareExchange(ref Buffers[current].InUse, 1, 0) == 0)
            {
                var tcs = Interlocked.Exchange(ref _waitNewEntry, new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously));
                // took the slot
                var slot = Buffers[current];
                slot.Buffer ??= new byte[count];
                if (slot.Buffer.Length < count)
                    slot.Buffer = new byte[count];

                Buffer.BlockCopy(buffer, 0, slot.Buffer, 0, count);
                slot.Size = count;

                tcs.TrySetResult(true);
                return;
            }

            if (waitingToken.IsCancellationRequested)
                return;

            current = (current + 1) % _size;
        }
    }

    public async Task<int> TryTake(byte[] buffer, int offset, int count, CancellationToken token)
    {
        var tcs = _waitNewEntry;
        try
        {
            await tcs.Task.WaitAsync(TimeSpan.FromMinutes(1), token).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            return -1;
        }

        var current = NextReadPosition;
        var slot = Buffers[current];

        var destFreeSpace = Math.Min(buffer.Length - offset, count);
        var remaining = slot.Remaining;
            
        if (remaining == 0)
            return 0;

        if (remaining > destFreeSpace)
        {
            Buffer.BlockCopy(slot.Buffer, slot.Consumed, buffer, offset, destFreeSpace);
            slot.Consumed += destFreeSpace;
            return destFreeSpace;
        }
        Buffer.BlockCopy(slot.Buffer, slot.Consumed, buffer, offset, remaining);

        NextReadPosition = (NextReadPosition + 1) % _size;
            
        if (closed)
            return remaining;

        slot.InUse = 0;
        _waitForFreeBuffer.Release();

        return remaining;
    }

    private bool closed;

    public void Close()
    {
        closed = true;

        using (_waitForFreeBuffer)
        using (_closeTokenSource)
        {
            _closeTokenSource.Cancel(throwOnFirstException: false);
            _waitNewEntry.TrySetResult(false);
        }
    }

    private class BufferHolder
    {
        public byte[] Buffer;
        public int Consumed;
        public int Size;
        public int InUse;
        public int Remaining => Size - Consumed;
    }
}
