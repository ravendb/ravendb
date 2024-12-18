using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Json.Parsing;
using static Sparrow.PortableExceptions;

namespace Sparrow.Json
{
    public enum BlittableVectorType : byte
    {
        SByte = 0b0000_0001,  // 1 byte
        Int16 = 0b0000_0010,  // 2 bytes
        Int32 = 0b0000_0100,  // 4 bytes
        Int64 = 0b0000_1000,  // 8 bytes

        Byte = 0b1000_0001,  // 1 byte
        UInt16 = 0b1000_0010,  // 2 bytes
        UInt32 = 0b1000_0100,  // 4 bytes
        UInt64 = 0b1000_1000,  // 8 bytes
        
        Half =   0b1100_0010, // 2 bytes
        Float =  0b1100_0100, // 4 bytes
        Double = 0b1100_1000, // 8 bytes
    }


    [StructLayout(LayoutKind.Explicit, Size = 8)]
    internal struct BlittableVectorHeader
    {
        private const byte TypeMask = 0b0011_1111;
        private const byte FloatFlag = 0b0100_0000;
        private const byte UnsignedFlag = 0b1000_0000;

        [FieldOffset(0)]
        private BlittableVectorType _type;

        [FieldOffset(1)]
        public byte AlignmentOffset;

        [FieldOffset(2)]
        public int Count;

        public BlittableVectorHeader(BlittableVectorType type, int count)
        {
            _type = type;
            Count = count;
        }

        public BlittableVectorType Type => _type;

        public bool IsFloatingPoint => (BlittableVectorType)((byte)_type & FloatFlag) != 0;

        public bool IsUnsigned => (BlittableVectorType)((byte)_type & UnsignedFlag) != 0;

        public int ElementSize => ((byte)Type) & TypeMask;
    }
    
    public sealed unsafe class BlittableJsonReaderVector : BlittableJsonReaderBase, IEnumerable
    {
        private readonly BlittableVectorHeader* _header;
        private readonly byte* _dataStart;

        public int Length => _header->Count;

        public BlittableVectorType Type => _header->Type;

        public int ElementSize => _header->ElementSize;
        
        public DynamicJsonArray Modifications;

        public BlittableJsonReaderVector(byte* mem, int bufferSize, JsonOperationContext context)
            : base(context)
        {
            //otherwise SetupPropertiesAccess will throw because of the memory garbage
            //(or won't throw, but this is actually worse!)
            ThrowIf<ArgumentException>(bufferSize == 0, $"{nameof(BlittableJsonReaderObject)} does not support objects with zero size");

            _mem = mem;
            _header = (BlittableVectorHeader*)mem; // +1 to skip the token byte
            _dataStart = mem + sizeof(BlittableVectorHeader) + _header->AlignmentOffset;
        }

        public object this[int i]
        {
            get
            {
                switch (_header->Type)
                {
                    case BlittableVectorType.SByte:
                        return *((sbyte*)_dataStart + i);
                    case BlittableVectorType.Int16:
                        return *((short*)_dataStart + i);
                    case BlittableVectorType.Int32:
                        return *((int*)_dataStart + i);
                    case BlittableVectorType.Int64:
                        return *((long*)_dataStart + i);
                    case BlittableVectorType.Byte:
                        return *((byte*)_dataStart + i);
                    case BlittableVectorType.UInt16:
                        return *((ushort*)_dataStart + i);
                    case BlittableVectorType.UInt32:
                        return *((uint*)_dataStart + i);
                    case BlittableVectorType.UInt64:
                        return *((ulong*)_dataStart + i);
#if NET6_0_OR_GREATER
                    case BlittableVectorType.Half:
                        return *((Half*)_dataStart + i);
#endif
                    case BlittableVectorType.Float:
                        return *((float*)_dataStart + i);
                    case BlittableVectorType.Double:
                        return *((double*)_dataStart + i);
                }

                throw new InvalidOperationException($"Vector is not of any known type {_header->Type}");
            }
        }

        public bool IsOfType<T>()
        {
            var type = typeof(T);
            switch (_header->Type)
            {
                case BlittableVectorType.SByte:
                    return type == typeof(sbyte);
                case BlittableVectorType.Int16:
                    return type == typeof(short);
                case BlittableVectorType.Int32:
                    return type == typeof(int);
                case BlittableVectorType.Int64:
                    return type == typeof(long);
                case BlittableVectorType.Byte:
                    return type == typeof(byte);
                case BlittableVectorType.UInt16:
                    return type == typeof(ushort);
                case BlittableVectorType.UInt32:
                    return type == typeof(uint);
                case BlittableVectorType.UInt64:
                    return type == typeof(ulong);
#if NET6_0_OR_GREATER
                case BlittableVectorType.Half:
                    return type == typeof(Half);
#endif
                case BlittableVectorType.Float:
                    return type == typeof(float);
                case BlittableVectorType.Double:
                    return type == typeof(double);
                default:
                    return false;
            }
        }

        /// <summary>
        /// Useful when we've to copy memory without known type. 
        /// </summary>
        /// <returns>Span of underlying memory</returns>
        public ReadOnlySpan<byte> ReadUnderlyingMemory()
        {
            return new ReadOnlySpan<byte>(_dataStart, _header->Count * _header->ElementSize);
        }

        public ReadOnlySpan<T> ReadArray<T>()
            where T : unmanaged
        {
            if (!IsOfType<T>())
                throw new InvalidOperationException($"Vector is not of type {typeof(T).Name}");

            return new ReadOnlySpan<T>(_dataStart, _header->Count);
        }

        private struct EnumerateAsLong<T> : IEnumerator<long> where T : unmanaged
        {
            private readonly byte* _dataStart;
            private readonly int _dataLength;
            private int _position;

            public EnumerateAsLong(byte* dataStart, int dataLength)
            {
                _dataStart = dataStart;
                _dataLength = dataLength;
            }

            public bool MoveNext()
            {
                _position++;
                return _position < _dataLength;
            }

            public void Reset()
            {
                _position = 0;
            }

            public long Current
            {
                get
                {
                    if (typeof(T) == typeof(sbyte))
                        return *((sbyte*)_dataStart + _position);
                    if (typeof(T) == typeof(byte))
                        return *((byte*)_dataStart + _position);

                    if (typeof(T) == typeof(short))
                        return *((short*)_dataStart + _position);
                    if (typeof(T) == typeof(ushort))
                        return *((ushort*)_dataStart + _position);

                    if (typeof(T) == typeof(int))
                        return *((int*)_dataStart + _position);
                    if (typeof(T) == typeof(uint))
                        return *((uint*)_dataStart + _position);

                    if (typeof(T) == typeof(ulong))
                        return (long)*((ulong*)_dataStart + _position);

                    return *((long*)_dataStart + _position);
                }
            }

            object IEnumerator.Current => Current;

            public void Dispose() { }
        }

        private struct EnumerateAsULong<T> : IEnumerator<ulong> where T : unmanaged
        {
            private readonly byte* _dataStart;
            private readonly int _dataLength;
            private int _position;

            public EnumerateAsULong(byte* dataStart, int dataLength)
            {
                _dataStart = dataStart;
                _dataLength = dataLength;
            }

            public bool MoveNext()
            {
                _position++;
                return _position < _dataLength;
            }

            public void Reset()
            {
                _position = 0;
            }

            public ulong Current
            {
                get
                {
                    if (typeof(T) == typeof(byte))
                        return *((byte*)_dataStart + _position);
                    if (typeof(T) == typeof(ushort))
                        return *((ushort*)_dataStart + _position);
                    if (typeof(T) == typeof(uint))
                        return *((uint*)_dataStart + _position);

                    return *((ulong*)_dataStart + _position);
                }
            }

            object IEnumerator.Current => Current;

            public void Dispose() { }
        }

        private struct EnumerateAsDouble<T> : IEnumerator<double> where T : unmanaged
        {
            private readonly byte* _dataStart;
            private readonly int _dataLength;
            private int _position;

            public EnumerateAsDouble(byte* dataStart, int dataLength)
            {
                _dataStart = dataStart;
                _dataLength = dataLength;
            }

            public bool MoveNext()
            {
                _position++;
                return _position < _dataLength;
            }

            public void Reset()
            {
                _position = 0;
            }

            public double Current
            {
                get
                {
#if NET6_0_OR_GREATER                    
                    if (typeof(T) == typeof(Half))
                        return (double)*((Half*)_dataStart + _position);
#endif
                    if (typeof(T) == typeof(float))
                        return *((float*)_dataStart + _position);
                    return *((double*)_dataStart + _position);
                }
            }

            object IEnumerator.Current => Current;

            public void Dispose() { }
        }

        public IEnumerator GetEnumerator()
        {
            switch (_header->Type)
            {
                case BlittableVectorType.SByte:
                    return new EnumerateAsLong<sbyte>(_dataStart, _header->Count);
                case BlittableVectorType.Int16:
                    return new EnumerateAsLong<short>(_dataStart, _header->Count);
                case BlittableVectorType.Int32:
                    return new EnumerateAsLong<int>(_dataStart, _header->Count);
                case BlittableVectorType.Int64:
                    return new EnumerateAsLong<long>(_dataStart, _header->Count);
                case BlittableVectorType.Byte:
                    return new EnumerateAsLong<byte>(_dataStart, _header->Count);
                case BlittableVectorType.UInt16:
                    return new EnumerateAsLong<ushort>(_dataStart, _header->Count);
                case BlittableVectorType.UInt32:
                    return new EnumerateAsLong<uint>(_dataStart, _header->Count);
                case BlittableVectorType.UInt64:
                    return new EnumerateAsULong<ulong>(_dataStart, _header->Count);
#if NET6_0_OR_GREATER
                case BlittableVectorType.Half:
                    return new EnumerateAsDouble<Half>(_dataStart, _header->Count);
#endif
                case BlittableVectorType.Float:
                    return new EnumerateAsDouble<float>(_dataStart, _header->Count);
                case BlittableVectorType.Double:
                    return new EnumerateAsDouble<double>(_dataStart, _header->Count);
            }

            throw new NotSupportedException("The type is not supported.");
        }
    }
}
