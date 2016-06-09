using System;

namespace Sparrow.Json
{
    public unsafe class BlittableJsonReaderBase
    {
        protected BlittableJsonReaderObject _parent;
        protected byte* _mem;
        protected int _size;
        protected byte* _propNames;
        protected int _propNamesDataOffsetSize;
        protected JsonOperationContext _context;

        public int ProcessTokenPropertyFlags(BlittableJsonToken currentType)
        {
            // process part of byte flags that responsible for property ids sizes
            const BlittableJsonToken mask =
                BlittableJsonToken.PropertyIdSizeByte | 
                BlittableJsonToken.PropertyIdSizeShort |
                BlittableJsonToken.PropertyIdSizeInt;

            switch (currentType &mask)
            {
                case BlittableJsonToken.PropertyIdSizeByte:
                    return sizeof (byte);
                case BlittableJsonToken.PropertyIdSizeShort:
                    return sizeof(short);
                case BlittableJsonToken.PropertyIdSizeInt:
                    return sizeof(int);
                default:
                    throw new ArgumentException("Illegal offset size");
            }
        }

        public int ProcessTokenOffsetFlags(BlittableJsonToken currentType)
        {
            // process part of byte flags that responsible for offset sizes
            const BlittableJsonToken mask =
                BlittableJsonToken.OffsetSizeByte |
                BlittableJsonToken.OffsetSizeShort |
                BlittableJsonToken.OffsetSizeInt;
            switch (currentType &
                    mask)
            {
                case BlittableJsonToken.OffsetSizeByte:
                    return sizeof (byte);
                case BlittableJsonToken.OffsetSizeShort:
                    return sizeof (short);
                case BlittableJsonToken.OffsetSizeInt:
                    return sizeof (int);
                default:
                    throw new ArgumentException("Illegal offset size");
            }
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
                    return currentType & TypesMask;
                default:
                    throw new ArgumentException("Illegal type");
            }
        }

        public int ReadNumber(byte* value, long sizeOfValue)
        {
            int returnValue;
            switch (sizeOfValue)
            {
                case sizeof (int):
                    returnValue = *value;
                    returnValue |= *(value + 1) << 8;
                    returnValue |= *(value + 2) << 16;
                    returnValue |= *(value + 3) << 24;
                    return returnValue;
                case sizeof (short):
                    returnValue = *value;
                    returnValue |= *(value + 1) << 8;
                    return returnValue;
                case sizeof (byte):
                    returnValue = *value;
                    return returnValue;
                default:
                    throw new ArgumentException($"Unsupported size {sizeOfValue}");
            }
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

        public int ReadVariableSizeInt(int pos, out byte offset)
        {
            return ReadVariableSizeInt(_mem, pos, out offset);
        }

        public static int ReadVariableSizeInt(byte* buffer, ref int pos)
        {
            byte offset;
            var result = ReadVariableSizeInt(buffer, pos, out offset);
            pos += offset;
            return result;
        }

        public static int ReadVariableSizeInt(byte* buffer, int pos, out byte offset)
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
                    throw new FormatException("Bad variable size int");
                b = buffer[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
                offset++;
            } while ((b & 0x80) != 0);
            return count;
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
                    throw new FormatException("Bad variable size int");
                b = buffer[pos--];
                count |= (b & 0x7F) << shift;
                shift += 7;
                offset++;
            } while ((b & 0x80) != 0);
            return count;
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
                if (shift == 69)
                    throw new FormatException("Bad variable size int");
                b = _mem[pos++];
                count |= (ulong)(b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);

            // good handling for negative values via:
            // http://code.google.com/apis/protocolbuffers/docs/encoding.html#types

            return (long)(count >> 1) ^ -(long)(count & 1);
        }
    }
}