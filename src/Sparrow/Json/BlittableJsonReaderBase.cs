﻿using System;
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

        public int ProcessTokenPropertyFlags(BlittableJsonToken currentType)
        {
            // process part of byte flags that responsible for property ids sizes
            const BlittableJsonToken mask =
                BlittableJsonToken.PropertyIdSizeByte | 
                BlittableJsonToken.PropertyIdSizeShort |
                BlittableJsonToken.PropertyIdSizeInt;

            // PERF: Switch for this case will create if-then-else anyways. 
            //       So we order them explicitely based on knowledge.
            BlittableJsonToken current = currentType & mask;
            if (current == BlittableJsonToken.PropertyIdSizeInt)
                return sizeof(int);
            if (current == BlittableJsonToken.PropertyIdSizeByte)
                return sizeof(byte);
            if (current == BlittableJsonToken.PropertyIdSizeShort)
                return sizeof(short);

            ThrowInvalidOfsetSize(currentType);
            return -1;//will never happen
        }

        public int ProcessTokenOffsetFlags(BlittableJsonToken currentType)
        {
            // process part of byte flags that responsible for offset sizes
            const BlittableJsonToken mask =
                BlittableJsonToken.OffsetSizeByte |
                BlittableJsonToken.OffsetSizeShort |
                BlittableJsonToken.OffsetSizeInt;

            // PERF: Switch for this case will create if-then-else anyways. 
            //       So we order them explicitely based on knowledge.
            BlittableJsonToken current = currentType & mask;
            if (current == BlittableJsonToken.OffsetSizeInt)
                return sizeof(int);
            if (current == BlittableJsonToken.OffsetSizeByte)
                return sizeof(byte);
            if (current == BlittableJsonToken.OffsetSizeShort)
                return sizeof(short);

            ThrowInvalidOfsetSize(currentType);
            return -1; // will never happen
        }

        private static void ThrowInvalidOfsetSize(BlittableJsonToken currentType)
        {
            throw new ArgumentException($"Illegal offset size {currentType}");
        }

        public const BlittableJsonToken TypesMask =
                BlittableJsonToken.Boolean |
                BlittableJsonToken.Float |
                BlittableJsonToken.Integer |
                BlittableJsonToken.Null |
                BlittableJsonToken.StartArray |
                BlittableJsonToken.StartObject |
                BlittableJsonToken.String |
                BlittableJsonToken.CompressedString;

        public BlittableJsonToken ProcessTokenTypeFlags(BlittableJsonToken currentType)
        {
            switch (currentType & TypesMask)
            {
                case BlittableJsonToken.StartObject:
                case BlittableJsonToken.StartArray:
                case BlittableJsonToken.Integer:
                case BlittableJsonToken.Float:
                case BlittableJsonToken.String:
                case BlittableJsonToken.CompressedString:
                case BlittableJsonToken.Boolean:
                case BlittableJsonToken.Null:
                case BlittableJsonToken.EmbeddedBlittable:
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
            int returnValue = *value;
            if (sizeOfValue == sizeof(byte))
                return returnValue;

            returnValue |= *(value + 1) << 8;
            if (sizeOfValue == sizeof(short))
                return returnValue;

            returnValue |= *(value + 2) << 16;
            returnValue |= *(value + 3) << 24;
            if (sizeOfValue == sizeof(int))
                return returnValue;          

            ThrowInvalidSizeForNumber(sizeOfValue);
            return -1;// will never happen
        }

        private static void ThrowInvalidSizeForNumber(long sizeOfValue)
        {
            throw new ArgumentException($"Unsupported size {sizeOfValue}");
        }

        public BlittableJsonReaderObject ReadNestedObject(int pos)
        {
            byte offset;
            var size = ReadVariableSizeInt(pos, out offset);

            return new BlittableJsonReaderObject(_mem + pos + offset, size, _context);
        }

        public LazyStringValue ReadStringLazily(int pos)
        {
            byte offset;
            var size = ReadVariableSizeInt(pos, out offset);

            return new LazyStringValue(null, _mem + pos + offset, size, _context);
        }

        public LazyCompressedStringValue ReadCompressStringLazily(int pos)
        {
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

        public static int ReadVariableSizeInt(byte* buffer, int pos, out byte offset)
        {
            if (pos < 0)
                ThrowInvalidPosition(pos);

            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            // we assume that the value shouldn't be zero very often
            // because then we'll always take 5 bytes to store it
            offset = 0;
            int count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    goto Error; // PERF: Using goto to diminish the size of the loop.

                b = buffer[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
                offset++;
            }
            while ((b & 0x80) != 0);

            return count;

            Error:
            ThrowInvalidShift();
            return -1;
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
            int shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    goto Error; // PERF: Using goto to diminish the size of the loop.

                b = buffer[pos--];
                count |= (b & 0x7F) << shift;
                shift += 7;
                offset++;
            }
            while ((b & 0x80) != 0);
            return count;

            Error:
            ThrowInvalidShift();
            return -1;
        }

        protected long ReadVariableSizeLong(int pos)
        {
            // ReadAsync out an Int64 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.

            ulong count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 70)
                    goto Error; // PERF: Using goto to diminish the size of the loop.

                b = _mem[pos++];
                count |= (ulong)(b & 0x7F) << shift;
                shift += 7;
            }
            while ((b & 0x80) != 0);

            // good handling for negative values via:
            // http://code.google.com/apis/protocolbuffers/docs/encoding.html#types

            return (long)(count >> 1) ^ -(long)(count & 1);

            Error:
            ThrowInvalidShift();
            return -1;
        }
    }
}