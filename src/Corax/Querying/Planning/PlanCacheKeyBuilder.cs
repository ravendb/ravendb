#nullable enable

using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Security.Cryptography;

namespace Corax.Querying.Planning;

[SkipLocalsInit]
public ref struct PlanCacheKeyBuilder()
{
    private const int DigestSize = 32; // SHA256.HashSizeInBytes
    
    [InlineArray(Size)]
    private struct CacheKeyBuffer
    {
        // Most plan keys fit comfortably; longer payloads (e.g. large query strings) spill into the chained
        // flush in PlanCacheKeyBuilder rather than growing this buffer.
        public const int Size = 256;

        private byte _element;
    }
    
    private CacheKeyBuffer _scratch;
    private int _bytePosition = 0;

    private ulong _bitAccumulator = 0;
    private int _bitCount = 0;

    // The full 256-byte buffer over the inline array. Non-readonly so the returned span is writable; the span
    // never escapes the builder, so taking a ref into our own field is safe.
    private Span<byte> Buffer => MemoryMarshal.CreateSpan(ref Unsafe.As<CacheKeyBuffer, byte>(ref _scratch), CacheKeyBuffer.Size);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(int value, int bits)
    {
        Debug.Assert(
            bits is > 0 and < 32 && // cannot send 32 bits (it's an int, would be negative)
            (uint)value >>> bits == 0
        );
        int freeBits = 64 - _bitCount;
        if (bits >= freeBits)
        {
            _bitAccumulator |= (ulong)value << _bitCount;
            value >>>= freeBits;
            bits -= freeBits;
            AppendBitsToBuffer(8);
        }

        _bitAccumulator |= (ulong)value << _bitCount;
        _bitCount += bits;
    }

    // Append raw bytes (e.g. a string's UTF-16 payload) directly into the buffer. Any partially filled bit
    // accumulator is first flushed to a byte boundary (the unused high bits pad with zero), then the bytes are
    // copied verbatim.
    //
    // When the bytes do not fit, we fill what is left, fold the buffer into a 32-byte digest carried in the first slot (see Flush).
    public void Append(ReadOnlySpan<byte> bytes)
    {
        if (_bitCount > 0)
            AppendBitsToBuffer((_bitCount + 7) / 8);

        Span<byte> buffer = Buffer;
        while (bytes.Length > buffer.Length - _bytePosition)
        {
            int space = buffer.Length - _bytePosition;
            bytes[..space].CopyTo(buffer[_bytePosition..]);
            _bytePosition += space;
            bytes = bytes[space..];
            Flush();
        }

        bytes.CopyTo(buffer[_bytePosition..]);
        _bytePosition += bytes.Length;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void AppendBitsToBuffer(int bytes)
    {
        EnsureCapacity(sizeof(ulong));
        BinaryPrimitives.WriteUInt64LittleEndian(Buffer[_bytePosition..], _bitAccumulator);
        _bytePosition += bytes;
        _bitAccumulator = 0;
        _bitCount = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureCapacity(int count)
    {
        // A single Flush always frees enough room here: count is at most sizeof(ulong) and the buffer is far
        // larger than DigestSize + sizeof(ulong), so after Flush the tail is >= sizeof(ulong) bytes.
        if (_bytePosition + count > CacheKeyBuffer.Size)
            Flush();
    }

    // Fold everything written so far into a 32-byte digest, parked in the first slot of the buffer, and rewind
    // to just past it. The carried digest becomes the prefix of the next block, chaining the blocks together
    // (a Merkle-Damgaard-style construction). The final hash is not the same value as a single-shot SHA256 over
    // the whole stream, but it is deterministic and collision-resistant enough for a cache key.
    [MethodImpl(MethodImplOptions.NoInlining)]
    [SkipLocalsInit]
    private void Flush()
    {
        Span<byte> buffer = Buffer;
        Span<byte> digest = stackalloc byte[DigestSize];
        SHA256.HashData(buffer[.._bytePosition], digest);
        digest.CopyTo(buffer);
        _bytePosition = DigestSize;
    }

    [SkipLocalsInit]
    public Vector256<long> ToHash()
    {
        if (_bitCount > 0)
            AppendBitsToBuffer((_bitCount + 7) / 8);

        Span<byte> digest = stackalloc byte[SHA256.HashSizeInBytes];
        SHA256.HashData(Buffer[.._bytePosition], digest);

        return Vector256.Create(MemoryMarshal.Cast<byte, long>(digest));
    }
}
