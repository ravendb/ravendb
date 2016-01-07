using System;
using System.Runtime.InteropServices;
using System.Text;
using ConsoleApplication4;
using Raven.Server.Json;
using Sparrow;

namespace NewBlittable
{
    public unsafe class BlittableJsonReaderBase
    {
        internal byte* _mem;
        internal int _size;
        internal byte* _propNames;
        internal int _propNamesDataOffsetSize;
        internal RavenOperationContext _context;

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

        internal const BlittableJsonToken typesMask =
                BlittableJsonToken.Boolean |
                BlittableJsonToken.Float |
                BlittableJsonToken.Integer |
                BlittableJsonToken.Null |
                BlittableJsonToken.StartArray |
                BlittableJsonToken.StartObject |
                BlittableJsonToken.String |
                BlittableJsonToken.CompressedString;

        internal object GetObject(BlittableJsonToken type, int position)
        {
            
            switch (type & typesMask)
            {
                case BlittableJsonToken.StartObject:
                    return new BlittableJsonReaderObject(position, this, type);
                case BlittableJsonToken.StartArray:
                    return new BlittableJsonReaderArray(position, this, type);
                case BlittableJsonToken.Integer:
                    return ReadVariableSizeInteger(position);
                case BlittableJsonToken.String:
                    return ReadStringLazily(position);
                case BlittableJsonToken.CompressedString:
                    return ReadCompressStringLazily(position);
                case BlittableJsonToken.Boolean:
                    return ReadNumber(_mem + position, 1) == 0;
                case BlittableJsonToken.Null:
                    return null;
                case BlittableJsonToken.Float:
                    return (double)ReadVariableSizeInteger(position);
                default:
                    throw new ArgumentOutOfRangeException((type).ToString());
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
            // Read out an Int32 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            offset = 0;
            int count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 35)
                    throw new FormatException("Bad variable size int");
                b = _mem[pos++];
                count |= (b & 0x7F) << shift;
                shift += 7;
                offset++;
            } while ((b & 0x80) != 0);
            return count;
        }

        protected long ReadVariableSizeInteger(int pos)
        {
            // Read out an Int64 7 bits at a time.  The high bit 
            // of the byte when on means to continue reading more bytes.
            long count = 0;
            int shift = 0;
            byte b;
            do
            {
                if (shift == 69)
                    throw new FormatException("Bad variable size int");
                b = _mem[pos++];
                count |= (long)(b & 0x7F) << shift;
                shift += 7;
            } while ((b & 0x80) != 0);
            return count;
        }
    }
}