using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow.Compression;
using static Sparrow.DisposableExceptions;

namespace Sparrow.Json
{
    public abstract unsafe class BlittableJsonReaderBase : IDisposableQueryable
    {
        protected BlittableJsonReaderObject _parent;
        protected internal byte* _mem;
        protected internal JsonOperationContext _context;

        protected BlittableJsonReaderBase(JsonOperationContext context)
        {
            _context = context;

            ThrowIfDisposedOnDebug(this);
        }

        public bool BelongsToContext(JsonOperationContext context)
        {
            ThrowIfDisposedOnDebug(this);
            return context == _context;
        }

        bool IDisposableQueryable.IsDisposed => _context?.IsDisposed ?? false;


        public bool HasParent => _parent != null;

        public bool NoCache { get; set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static int ProcessTokenPropertyFlags(BlittableJsonToken currentType)
        {
            // process part of byte flags that responsible for property ids sizes
            const BlittableJsonToken mask =
                BlittableJsonToken.PropertyIdSizeByte | 
                BlittableJsonToken.PropertyIdSizeShort |
                BlittableJsonToken.PropertyIdSizeInt;

            // PERF: Switch for this case will create if-then-else anyways. 
            //       So we order them explicitly based on knowledge.
            BlittableJsonToken current = currentType & mask;
            int size; // PERF: We assign to a variable instead to have smaller code for inlining.
            if (current == BlittableJsonToken.PropertyIdSizeByte)
                size = sizeof(byte); 
            else if (current == BlittableJsonToken.PropertyIdSizeShort)
                size = sizeof(short);
            else if (current == BlittableJsonToken.PropertyIdSizeInt)
                size = sizeof(int);
            else
                size = ThrowInvalidOffsetSize(currentType);
                
            return size;                        
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static int ProcessTokenOffsetFlags(BlittableJsonToken currentType)
        {
            // process part of byte flags that responsible for offset sizes
            const BlittableJsonToken mask =
                BlittableJsonToken.OffsetSizeByte |
                BlittableJsonToken.OffsetSizeShort |
                BlittableJsonToken.OffsetSizeInt;

            // PERF: Switch for this case will create if-then-else anyways. 
            //       So we order them explicitly based on knowledge.
            BlittableJsonToken current = currentType & mask;
            int size; // PERF: We assign to a variable instead to have smaller code for inlining.
            if (current == BlittableJsonToken.OffsetSizeByte)
                size = sizeof(byte);
            else if (current == BlittableJsonToken.OffsetSizeShort)
                size = sizeof(short);
            else if (current == BlittableJsonToken.OffsetSizeInt)
                size = sizeof(int);
            else
                size = ThrowInvalidOffsetSize(currentType);

            return size;
        }

        private static int ThrowInvalidOffsetSize(BlittableJsonToken currentType)
        {
            throw new ArgumentException($"Illegal offset size {currentType}");
        }

        public const BlittableJsonToken TypesMask =
                BlittableJsonToken.Boolean |
                BlittableJsonToken.LazyNumber |
                BlittableJsonToken.Integer |
                BlittableJsonToken.Null |
                BlittableJsonToken.StartArray |
                BlittableJsonToken.StartObject |
                BlittableJsonToken.String |
                BlittableJsonToken.CompressedString;
        
        public static BlittableJsonToken ProcessTokenTypeFlags(BlittableJsonToken currentType)
        {
            var token = currentType & TypesMask;
            if (token is >= BlittableJsonToken.StartObject and <= BlittableJsonToken.RawBlob)
                return currentType & TypesMask;

            ThrowInvalidType(currentType);
            return default;// will never happen
        }

        private static void ThrowInvalidType(BlittableJsonToken currentType)
        {
            throw new ArgumentException($"Illegal type {currentType}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected int ReadNumber(byte* value, long sizeOfValue)
        {
            int returnValue = *value;
            if (sizeOfValue == sizeof(byte))
                goto Successful;

            returnValue |= *(value + 1) << 8;
            if (sizeOfValue == sizeof(short))
                goto Successful;

            returnValue |= *(short*)(value + 2) << 16;
            if (sizeOfValue == sizeof(int))
                goto Successful;

            goto Error;
            
            Successful:
            return returnValue;

            Error:
            return ThrowInvalidSizeForNumber(sizeOfValue);
        }

        private static int ThrowInvalidSizeForNumber(long sizeOfValue)
        {
            throw new ArgumentException($"Unsupported size {sizeOfValue}");
        }

        public BlittableJsonReaderObject ReadNestedObject(int pos)
        {
            ThrowIfDisposedOnDebug(this);
            
            var size = VariableSizeEncoding.Read<int>(_mem + pos, out var offset);
            return new BlittableJsonReaderObject(_mem + pos + offset, size, _context)
            {
                NoCache = NoCache
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue ReadStringLazily(int pos)
        {
            ThrowIfDisposedOnDebug(this);
            
            var size = VariableSizeEncoding.Read<int>(_mem + pos, out var offset);

            return _context.AllocateStringValue(null, _mem + pos + offset, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyCompressedStringValue ReadCompressStringLazily(int pos)
        {
            ThrowIfDisposedOnDebug(this);
            
            var uncompressedSize = VariableSizeEncoding.Read<int>(_mem + pos, out var offset);
            pos += offset;
            var compressedSize = VariableSizeEncoding.Read<int>(_mem + pos, out offset);
            pos += offset;
            return new LazyCompressedStringValue(null, _mem + pos, uncompressedSize, compressedSize, _context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadVariableSizeInt(int pos, out int offset)
        {
            return VariableSizeEncoding.Read<int>(_mem + pos, out offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadVariableSizeInt(byte* buffer, ref int pos)
        {
            var result = VariableSizeEncoding.Read<int>(buffer + pos, out var offset);
            pos += offset;
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadVariableSizeInt(byte* buffer, int pos, out int offset, out bool success)
        {
            return VariableSizeEncoding.Read<int>(buffer + pos, out offset, out success);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadVariableSizeInt(ReadOnlySpan<byte> buffer, int pos, out int offset, out bool success)
        {
            return VariableSizeEncoding.ReadCompact<int>(buffer, pos, out offset, out success);
        }

        private static void ThrowInvalidShift()
        {
            throw new FormatException("Bad variable size int");
        }

        public static int ReadVariableSizeIntInReverse(byte* buffer, int pos, out byte offset)
        {
            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            // we assume that the value shouldn't be zero very often
            // because then we'll always take 5 bytes to store it
            offset = 0;
            int count = 0;
            byte shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    goto Error; // PERF: Using goto to diminish the size of the loop.

                b = buffer[pos];
                pos--;
                offset++;

                count |= (b & 0x7F) << shift;
                shift += 7;                
            }
            while ((b & 0x80) != 0);
            return count;

            Error:
            ThrowInvalidShift();
            return -1;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected long ReadVariableSizeLong(int pos)
        {
            return ZigZagEncoding.Decode<long>(_mem, out _, pos: pos);
        }

        [Conditional("DEBUG")]
        protected void AssertContextNotDisposed(JsonOperationContext context)
        {
            if (context?.IsDisposed ?? false)
            {
                throw new ObjectDisposedException("blittable's context has been disposed, blittable should not be used now in that case!");
            }
        }
    }
}
