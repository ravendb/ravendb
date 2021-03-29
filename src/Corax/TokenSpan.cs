using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Collections;

namespace Corax
{
    public struct TokenPosition
    {
        public const int Invalid = -1;

        public readonly int Storage;
        public int Offset;

        /// <summary>
        /// The Token Position is encodes the storage index and the offset position in a single value.
        /// </summary>
        /// <param name="storageIndex"></param>
        /// <param name="position"></param>
        public TokenPosition(int storageIndex, int offset)
        {
            Debug.Assert(offset >= 0);

            Storage = storageIndex;
            Offset = offset;
        }

        public bool IsValid
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get { return Storage < 0; }
        }
    }

    public static class TokenType
    {
        public const int None = 0;
        public const int Word = 1;
        public const int Keyword = 2;
    }

    // A TokenSpan is a representation of data that is contiguous in memory and therefore can be accessed directly from the
    // TokenSpanStorageContext.
    public struct TokenSpan
    {
        public static readonly ObjectPool<FastList<TokenSpan>> SequencesPool = new(() => new FastList<TokenSpan>(16), 32);

        public static readonly TokenSpan Null = new TokenSpan(new TokenPosition(TokenPosition.Invalid, 0), 0);

        public TokenPosition Position;
        public int Length;
        public int Type;

        public TokenSpan(TokenPosition position, int length, int type = 0)
        {
            Position = position;
            Length = length;
            Type = type;
        }
    }

    // The TokenSpanStorageContext gives the whole system access to common resources like shared buffers
    // in order to ensure that work can be done as memory efficiently as possible. 
    public sealed class TokenSpanStorageContext : IDisposable
    {
        private readonly FastList<byte[]> _buffers = new(64);

        /// <summary>
        /// It allocates a new TokenSpan which 
        /// </summary>
        /// <param name="size"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Span<byte> Allocate(out TokenSpan token, int size, int type = TokenType.None)
        {
            var array = ArrayPool<byte>.Shared.Rent(size);

            int index = _buffers.Count;
            _buffers.Add(array);

            token = new TokenSpan(new TokenPosition(index, 0), size, type);
            return new(array, 0, size);
        }

        public void Return(ref TokenSpan token)
        {
            int index = token.Position.Storage;

            ArrayPool<byte>.Shared.Return(_buffers[index], clearArray: true);
            _buffers[index] = null;
        }

        public void Resize(ref TokenSpan token, int newSize)
        {
            int index = token.Position.Storage;

            var buffer = _buffers[index];
            if (buffer.Length > newSize)
            {
                // The array was big enough to handle the size already. 
                goto Done;
            }

            // Request a new buffer
            var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);

            // Copy the content to the new buffer
            var source = new Span<byte>(buffer, 0, token.Length);
            var destination = new Span<byte>(newBuffer, 0, token.Length);
            source.CopyTo(destination);

            // Return the old buffer to the pool and update the current buffer.
            ArrayPool<byte>.Shared.Return(buffer);
            _buffers[index] = newBuffer;

            Done:
            token = new TokenSpan(new TokenPosition(index, 0), newSize, token.Type);
        }

        public ReadOnlySpan<byte> RequestReadAccess(in TokenSpan token)
        {
            return new(_buffers[token.Position.Storage], token.Position.Offset, token.Length);
        }

        public Span<byte> RequestWriteAccess(in TokenSpan token)
        {
            return new(_buffers[token.Position.Storage], token.Position.Offset, token.Length);
        }

        public void Reset()
        {
        }

        public void Dispose()
        {
            Reset();
        }
    }
}
