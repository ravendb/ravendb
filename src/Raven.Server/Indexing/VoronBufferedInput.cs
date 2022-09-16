using System;
using System.Buffers;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Store;
using Raven.Client.Extensions.Streams;
using Voron.Data;
using Voron.Impl;

namespace Raven.Server.Indexing;

public class VoronBufferedInput : BufferedIndexInput
{
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();
    private const int DefaultBufferSize = 4096;
    private readonly LuceneVoronDirectory _directory;
    private readonly string _name;
    private readonly string _tree;
    private VoronStream _stream;
    private bool _isDisposed = false;

    private bool _isOriginal = true;

    /// <summary>Reads a long stored in variable-length format.  Reads between one and
    /// nine bytes.  Smaller values take fewer bytes.  Negative numbers are not
    /// supported. 
    /// </summary>
    public override long ReadVLong(IState state)
    {
        if (bufferPosition + 9 < bufferLength)
        {
            // handle directly
            byte b = buffer[bufferPosition++];
            int i = b & 0x7F;
            for (int shift = 7; (b & 0x80) != 0; shift += 7)
            {
                b = buffer[bufferPosition++];
                i |= (b & 0x7F) << shift;
            }

            return i;
        }

        return ReadVLongUnlikely(state);
    }

    private long ReadVLongUnlikely(IState state)
    {
        //We want to refill only when we're out of cache. Calling this before can lead to data loss. 
        if (bufferPosition >= bufferLength)
        {
            Refill(state);
            if (bufferPosition + 9 < bufferLength)
            {
                return ReadVLong(state);
            }
        }

        // We don't have 9 elements (what is max here) in buffer, just go to standard implementation
        // that would read it one line at a time
        return base.ReadVLong(state);
    }

    /// <summary>Reads an int stored in variable-length format.  Reads between one and
    /// five bytes.  Smaller values take fewer bytes.  Negative numbers are not
    /// supported.
    /// </summary>
    /// <seealso cref="IndexOutput.WriteVInt(int)">
    /// </seealso>
    public override int ReadVInt(IState state)
    {
        if (bufferPosition + 5 < bufferLength)
        {
            // handle directly
            byte b = buffer[bufferPosition++];
            int i = b & 0x7F;
            for (int shift = 7; (b & 0x80) != 0; shift += 7)
            {
                b = buffer[bufferPosition++];
                i |= (b & 0x7F) << shift;
            }

            return i;
        }

        return ReadVIntUnlikely(state);
    }

    private int ReadVIntUnlikely(IState state)
    {
        //We want to refill only when we're out of cache. Calling this before can lead to data loss. 
        if (bufferPosition >= bufferLength)
        {
            Refill(state);
            if (bufferPosition + 5 < bufferLength)
            {
                return ReadVInt(state);
            }
        }

        // We don't have 5 elements (what is max here) in buffer, just go to standard implementation
        // that would read it one line at a time
        return base.ReadVInt(state);
    }

    public VoronBufferedInput(LuceneVoronDirectory directory, string name, Transaction transaction, string tree) : base(DefaultBufferSize)
    {
        _directory = directory;
        _name = name;
        _tree = tree;

        _stream = VoronIndexInput.OpenVoronStream(transaction, _directory, _name, _tree);

        //We don't want to get buffer instantly. Only when needed.
        //buffer = ArrayPool<byte>.Shared.Rent(DefaultBufferSize);
    }

    public override string ToString()
    {
        return _name;
    }

    public override void SetBufferSize(int newSize)
    {
        if (buffer == null)
            return;

        if (newSize <= buffer.Length)
            return;

        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        Array.Copy(buffer, newBuffer, buffer.Length);
        var oldBuffer = buffer;
        buffer = newBuffer;
        ArrayPool<byte>.Shared.Return(oldBuffer);
    }

    public override object Clone(IState s)
    {
        var state = s as VoronState;
        if (state == null)
        {
            ThrowStateNullException();
            return null;
        }

        ThrowIfDisposed(state);

        var clone = (VoronBufferedInput)base.Clone(s);
        GC.SuppressFinalize(clone);
        clone._isOriginal = false;
        clone._stream = VoronIndexInput.OpenVoronStream(state.Transaction, clone._directory, clone._name, clone._tree);
        clone._stream.Position = _stream.Position;

        return clone;
    }

    protected override void Dispose(bool disposing)
    {
        if (_isDisposed)
            return;

        GC.SuppressFinalize(this);

        if (buffer != null)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (_isOriginal == false)
            return;

        _cts.Cancel();
        _cts.Dispose();


        _isDisposed = true;
    }

    public override long Length(IState s)
    {
        if (s is VoronState)
        {
            return _stream.Length;
        }

        ThrowStateNullException();
        return 0;
    }


    public override void ReadInternal(byte[] b, int offset, int length, IState s)
    {
        var state = s as VoronState;
        if (state == null)
        {
            ThrowStateNullException();
            return;
        }

        ThrowIfDisposed(state);

        _stream.UpdateCurrentTransaction(state.Transaction);
        _stream.ReadEntireBlock(b, offset, length);
    }

    public override void SeekInternal(long pos, IState s)
    {
        if (pos > _stream.Length)
            ThrowInvalidSeekPosition(pos);

        if (s is not VoronState state)
        {
            ThrowStateNullException();
            return;
        }

        ThrowIfDisposed(state);

        _stream.Seek(pos, SeekOrigin.Begin);
    }

    protected override void NewBuffer(int bufferSize)
    {
        buffer = ArrayPool<byte>.Shared.Rent(Math.Max(DefaultBufferSize, bufferSize));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed(VoronState state)
    {
        if (state == null)
        {
            ThrowDisposed();
            return; // never hit
        }

        if (state.Transaction.LowLevelTransaction.IsDisposed)
            ThrowTransactionDisposed();
        if (_cts.IsCancellationRequested)
            ThrowCancelled();
    }


    private static void ThrowTransactionDisposed()
    {
        throw new ObjectDisposedException("No Transaction in thread");
    }

    private static void ThrowDisposed()
    {
        throw new ObjectDisposedException("No Transaction in thread");
    }

    private static void ThrowCancelled()
    {
        throw new OperationCanceledException($"{nameof(VoronBufferedInput)}");
    }

    private static void ThrowStateNullException()
    {
        throw new ArgumentNullException("State");
    }

    private void ThrowEndOfStreamException()
    {
        throw new EndOfStreamException($"Input name: {_name}. Current position: {_stream.Position}, length: {_stream.Length}");
    }

    private void ThrowInvalidSeekPosition(long pos)
    {
        throw new InvalidOperationException($"Cannot set stream position to {pos} because the length of '{_name}' stream is {_stream.Length}");
    }
}
