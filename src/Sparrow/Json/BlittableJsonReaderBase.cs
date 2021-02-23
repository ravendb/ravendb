using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Sparrow.Json
{
    public abstract unsafe class BlittableJsonReaderBase
    {
        protected BlittableJsonReaderObject _parent;
        protected byte* _mem;
        protected byte* _propNames;
        protected int _propNamesDataOffsetSize;
        protected internal JsonOperationContext _context;

        protected BlittableJsonReaderBase(JsonOperationContext context)
        {
            _context = context;
            AssertContextNotDisposed();
        }

        public bool BelongsToContext(JsonOperationContext context)
        {
            AssertContextNotDisposed();
            return context == _context;
        }

        public bool HasParent => _parent != null;

        public bool NoCache { get; set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ProcessTokenPropertyFlags(BlittableJsonToken currentType)
        {
            AssertContextNotDisposed();
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
        public int ProcessTokenOffsetFlags(BlittableJsonToken currentType)
        {
            AssertContextNotDisposed();
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

        public BlittableJsonToken ProcessTokenTypeFlags(BlittableJsonToken currentType)
        {
            AssertContextNotDisposed();
            switch (currentType & TypesMask)
            {
                case BlittableJsonToken.StartObject:
                case BlittableJsonToken.StartArray:
                case BlittableJsonToken.Integer:
                case BlittableJsonToken.LazyNumber:
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                case BlittableJsonToken.Boolean:
                case BlittableJsonToken.Null:
                case BlittableJsonToken.EmbeddedBlittable:
                case BlittableJsonToken.RawBlob:
                    return currentType & TypesMask;
                default:
                    ThrowInvalidType(currentType);
                    return default(BlittableJsonToken);// will never happen
            }
        }

        private static void ThrowInvalidType(BlittableJsonToken currentType)
        {
            throw new ArgumentException($"Illegal type {currentType}");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadNumber(byte* value, long sizeOfValue)
        {
            AssertContextNotDisposed();
            int returnValue = *value;
            if (sizeOfValue == sizeof(byte))
                return returnValue;

            returnValue |= *(value + 1) << 8;
            if (sizeOfValue == sizeof(short))
                return returnValue;

            returnValue |= *(short*)(value + 2) << 16;
            if (sizeOfValue == sizeof(int))
                return returnValue;

            return ThrowInvalidSizeForNumber(sizeOfValue);
        }

        private static int ThrowInvalidSizeForNumber(long sizeOfValue)
        {
            throw new ArgumentException($"Unsupported size {sizeOfValue}");
        }

        public BlittableJsonReaderObject ReadNestedObject(int pos)
        {
            AssertContextNotDisposed();
            byte offset;
            var size = ReadVariableSizeInt(pos, out offset);
            return new BlittableJsonReaderObject(_mem + pos + offset, size, _context)
            {
                NoCache = NoCache
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyStringValue ReadStringLazily(int pos)
        {
            AssertContextNotDisposed();
            byte offset;
            var size = ReadVariableSizeInt(pos, out offset);

            return _context.AllocateStringValue(null, _mem + pos + offset, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LazyCompressedStringValue ReadCompressStringLazily(int pos)
        {
            AssertContextNotDisposed();
            byte offset;
            var uncompressedSize = ReadVariableSizeInt(pos, out offset);
            pos += offset;
            var compressedSize = ReadVariableSizeInt(pos, out offset);
            pos += offset;
            return new LazyCompressedStringValue(null, _mem + pos, uncompressedSize, compressedSize, _context);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ReadVariableSizeInt(int pos, out byte offset)
        {
            return ReadVariableSizeInt(_mem, pos, out offset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadVariableSizeInt(byte* buffer, ref int pos)
        {
            byte offset;
            var result = ReadVariableSizeInt(buffer, pos, out offset);
            pos += offset;
            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int ReadVariableSizeInt(byte* buffer, int pos, out byte offset)
        {
            offset = 0;

            if (pos < 0)
                ThrowInvalidPosition(pos);

            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            // we assume that the value shouldn't be zero very often
            // because then we'll always take 5 bytes to store it

            int count = 0;
            byte shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    ThrowInvalidShift();

                b = buffer[pos];
                pos++;
                offset++;

                count |= (b & 0x7F) << shift;
                shift += 7;                
            }
            while ((b & 0x80) != 0);

            return count;
        }

        private static void ThrowInvalidShift()
        {
            throw new FormatException("Bad variable size int");
        }

        private static void ThrowInvalidPosition(int pos)
        {
            throw new ArgumentOutOfRangeException(nameof(pos), "Position cannot be negative, but was " + pos);
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
                    ThrowInvalidShift();

                b = buffer[pos];
                pos--;
                offset++;

                count |= (b & 0x7F) << shift;
                shift += 7;                
            }
            while ((b & 0x80) != 0);
            return count;
        }

        protected long ReadVariableSizeLong(int pos)
        {
            AssertContextNotDisposed();
            // ReadAsync out an Int64 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.

            ulong count = 0;
            byte shift = 0;
            byte b;
            do
            {
                if (shift == 70)
                    ThrowInvalidShift();

                b = _mem[pos++];
                count |= (ulong)(b & 0x7F) << shift;
                shift += 7;
            }
            while ((b & 0x80) != 0);

            // good handling for negative values via:
            // http://code.google.com/apis/protocolbuffers/docs/encoding.html#types

            return (long)(count >> 1) ^ -(long)(count & 1);
        }

        [Conditional("DEBUG")]
        protected void AssertContextNotDisposed()
        {
            if (_context?.Disposed ?? false)
            {
                throw new ObjectDisposedException("blittable's context has been disposed, blittable should not be used now in that case!");
            }
        }
    }
}
