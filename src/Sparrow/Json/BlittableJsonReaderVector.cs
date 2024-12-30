using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
    
    public sealed unsafe class BlittableJsonReaderVector : BlittableJsonReaderBase
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

        public bool IsTypeCompatibleForDirectRead<T>()
        {
            var type = typeof(T);
            return _header->Type switch
            {
                BlittableVectorType.Byte or BlittableVectorType.SByte => type == typeof(sbyte) || type == typeof(byte),
                BlittableVectorType.Int16 => type == typeof(short) || type == typeof(ushort),
                BlittableVectorType.Int32 => type == typeof(int) || type == typeof(uint),
                BlittableVectorType.Int64 => type == typeof(long) || type == typeof(ulong),
                BlittableVectorType.UInt16 => type == typeof(ushort) || type == typeof(short),
                BlittableVectorType.UInt32 => type == typeof(uint) || type == typeof(int),
                BlittableVectorType.UInt64 => type == typeof(ulong) || type == typeof(long),
                BlittableVectorType.Float => type == typeof(float),
                BlittableVectorType.Double => type == typeof(double),
                #if NET6_0_OR_GREATER
                BlittableVectorType.Half => type == typeof(Half),
                #endif
                _ => false
            };

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
            if (IsOfType<T>() == false)
                throw new InvalidOperationException($"Vector is not of type {typeof(T).Name}");

            return new ReadOnlySpan<T>(_dataStart, _header->Count);
        }

        public bool TryReadArray<T>(out ReadOnlySpan<T> span)
        {
            if (IsTypeCompatibleForDirectRead<T>() == false)
            {
                span = [];
                return false;
            }

            span = new ReadOnlySpan<T>(_dataStart, _header->Count);
            return true;
        }

        public EnumerableAs<TOut> ReadAs<TOut>() where TOut : unmanaged
        {
            var size = _header->Count * _header->ElementSize; 
            return new EnumerableAs<TOut>(_dataStart, size, ElementSize, _header->Type);
        }

        /**
         * Since compression may happen on the server side, we need a simple way to deserialize it on the client side.
         * Let's introduce a method that reads the requested type.
         **/
        public struct EnumerableAs<TOut> : IEnumerator<TOut>
            where TOut : unmanaged
        {
            private readonly BlittableVectorType _type;
            public int Count => _dataLength / _elementSize;
            private int _position = -1;
            private readonly byte* _dataStart;
            private readonly int _dataLength;
            private readonly int _elementSize;
            private readonly delegate*<byte*, TOut> _getElement;
  
            public EnumerableAs(byte* dataStart, int dataLength, int elementSize, BlittableVectorType type)
            {
                _dataStart = dataStart;
                _dataLength = dataLength;
                _elementSize = elementSize;
                
                // This might be expensive; however, the method should not be used extensively.
                _getElement = type switch
                {
                    BlittableVectorType.SByte => &SbyteConverter,
                    BlittableVectorType.Int16 => &Int16Converter,
                    BlittableVectorType.Int32 => &Int32Converter,
                    BlittableVectorType.Int64 => &UInt64Converter,
                    
                    BlittableVectorType.Byte => &ByteConverter,
                    BlittableVectorType.UInt16 => &UInt16Converter,
                    BlittableVectorType.UInt32 => &UInt32Converter,
                    BlittableVectorType.UInt64 => &UInt64Converter,
                    
                    BlittableVectorType.Float => &FloatConverter,
                    BlittableVectorType.Double => &DoubleConverter,
#if NET6_0_OR_GREATER
                    BlittableVectorType.Half => &HalfConverter,
#endif
                    _ => throw new NotSupportedException($"The type `{type}` is not supported.")
                };
            }

            public bool MoveNext()
            {
                _position++;
                return _position * _elementSize < _dataLength;
            }

            public void Reset()
            {
                _position = -1;
            }

            public TOut Current
            {
                get
                {
                    var currentElementPtr = (_dataStart + (_position * _elementSize));
                    return _getElement(currentElementPtr);
                }
            }

            object IEnumerator.Current => Current;

            private static TOut SbyteConverter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<sbyte>(data);
                
                if (typeof(TOut) == typeof(sbyte))
                    return (TOut)(object)Convert.ToSByte(value);
                if (typeof(TOut) == typeof(short))
                    return (TOut)(object)Convert.ToInt16(value);
                if (typeof(TOut) == typeof(int))
                    return (TOut)(object)Convert.ToInt32(value);
                if (typeof(TOut) == typeof(long))
                    return (TOut)(object)Convert.ToInt64(value);
                
                if (typeof(TOut) == typeof(byte))
                    return (TOut)(object)Convert.ToByte(value);
                if (typeof(TOut) == typeof(ushort))
                    return (TOut)(object)Convert.ToUInt16(value);
                if (typeof(TOut) == typeof(uint))
                    return (TOut)(object)Convert.ToUInt32(value);
                if (typeof(TOut) == typeof(ulong))
                    return (TOut)(object)Convert.ToUInt64(value);
                
                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)Convert.ToSingle(value);
                if (typeof(TOut) == typeof(double))
                    return (TOut)(object)Convert.ToDouble(value);
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)Convert.ToSingle(value);
#endif
                
                return (TOut)(object)value;
            }
            
            private static TOut ByteConverter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<byte>(data);
                
                if (typeof(TOut) == typeof(sbyte))
                    return (TOut)(object)Convert.ToSByte(value);
                if (typeof(TOut) == typeof(short))
                    return (TOut)(object)Convert.ToInt16(value);
                if (typeof(TOut) == typeof(int))
                    return (TOut)(object)Convert.ToInt32(value);
                if (typeof(TOut) == typeof(long))
                    return (TOut)(object)Convert.ToInt64(value);
                
                if (typeof(TOut) == typeof(byte))
                    return (TOut)(object)Convert.ToByte(value);
                if (typeof(TOut) == typeof(ushort))
                    return (TOut)(object)Convert.ToUInt16(value);
                if (typeof(TOut) == typeof(uint))
                    return (TOut)(object)Convert.ToUInt32(value);
                if (typeof(TOut) == typeof(ulong))
                    return (TOut)(object)Convert.ToUInt64(value);
                
                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)Convert.ToSingle(value);
                if (typeof(TOut) == typeof(double))
                    return (TOut)(object)Convert.ToDouble(value);
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)Convert.ToSingle(value);
#endif
                
                return (TOut)(object)value;
            }

            private static TOut Int16Converter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<short>(data);

                if (typeof(TOut) == typeof(sbyte))
                    return (TOut)(object)Convert.ToSByte(value);
                if (typeof(TOut) == typeof(short))
                    return (TOut)(object)Convert.ToInt16(value);
                if (typeof(TOut) == typeof(int))
                    return (TOut)(object)Convert.ToInt32(value);
                if (typeof(TOut) == typeof(long))
                    return (TOut)(object)Convert.ToInt64(value);
                
                if (typeof(TOut) == typeof(byte))
                    return (TOut)(object)Convert.ToByte(value);
                if (typeof(TOut) == typeof(ushort))
                    return (TOut)(object)Convert.ToUInt16(value);
                if (typeof(TOut) == typeof(uint))
                    return (TOut)(object)Convert.ToUInt32(value);
                if (typeof(TOut) == typeof(ulong))
                    return (TOut)(object)Convert.ToUInt64(value);
                
                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)Convert.ToSingle(value);
                if (typeof(TOut) == typeof(double))
                    return (TOut)(object)Convert.ToDouble(value);
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)Convert.ToSingle(value);
#endif
                
                return (TOut)(object)value;
            }
            
            private static TOut UInt16Converter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<ushort>(data);
                

                if (typeof(TOut) == typeof(sbyte))
                    return (TOut)(object)Convert.ToSByte(value);
                if (typeof(TOut) == typeof(short))
                    return (TOut)(object)Convert.ToInt16(value);
                if (typeof(TOut) == typeof(int))
                    return (TOut)(object)Convert.ToInt32(value);
                if (typeof(TOut) == typeof(long))
                    return (TOut)(object)Convert.ToInt64(value);
                
                if (typeof(TOut) == typeof(byte))
                    return (TOut)(object)Convert.ToByte(value);
                if (typeof(TOut) == typeof(ushort))
                    return (TOut)(object)Convert.ToUInt16(value);
                if (typeof(TOut) == typeof(uint))
                    return (TOut)(object)Convert.ToUInt32(value);
                if (typeof(TOut) == typeof(ulong))
                    return (TOut)(object)Convert.ToUInt64(value);
                
                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)Convert.ToSingle(value);
                if (typeof(TOut) == typeof(double))
                    return (TOut)(object)Convert.ToDouble(value);
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)Convert.ToSingle(value);
#endif
                
                return (TOut)(object)value;
                
                return (TOut)(object)value;
            }
            
            private static TOut UInt32Converter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<uint>(data);
                

                if (typeof(TOut) == typeof(sbyte))
                    return (TOut)(object)Convert.ToSByte(value);
                if (typeof(TOut) == typeof(short))
                    return (TOut)(object)Convert.ToInt16(value);
                if (typeof(TOut) == typeof(int))
                    return (TOut)(object)Convert.ToInt32(value);
                if (typeof(TOut) == typeof(long))
                    return (TOut)(object)Convert.ToInt64(value);
                
                if (typeof(TOut) == typeof(byte))
                    return (TOut)(object)Convert.ToByte(value);
                if (typeof(TOut) == typeof(ushort))
                    return (TOut)(object)Convert.ToUInt16(value);
                if (typeof(TOut) == typeof(uint))
                    return (TOut)(object)Convert.ToUInt32(value);
                if (typeof(TOut) == typeof(ulong))
                    return (TOut)(object)Convert.ToUInt64(value);
                
                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)Convert.ToSingle(value);
                if (typeof(TOut) == typeof(double))
                    return (TOut)(object)Convert.ToDouble(value);
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)Convert.ToSingle(value);
#endif
                
                return (TOut)(object)value;
            }
            
            private static TOut Int32Converter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<uint>(data);


                if (typeof(TOut) == typeof(sbyte))
                    return (TOut)(object)Convert.ToSByte(value);
                if (typeof(TOut) == typeof(short))
                    return (TOut)(object)Convert.ToInt16(value);
                if (typeof(TOut) == typeof(int))
                    return (TOut)(object)Convert.ToInt32(value);
                if (typeof(TOut) == typeof(long))
                    return (TOut)(object)Convert.ToInt64(value);
                
                if (typeof(TOut) == typeof(byte))
                    return (TOut)(object)Convert.ToByte(value);
                if (typeof(TOut) == typeof(ushort))
                    return (TOut)(object)Convert.ToUInt16(value);
                if (typeof(TOut) == typeof(uint))
                    return (TOut)(object)Convert.ToUInt32(value);
                if (typeof(TOut) == typeof(ulong))
                    return (TOut)(object)Convert.ToUInt64(value);
                
                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)Convert.ToSingle(value);
                if (typeof(TOut) == typeof(double))
                    return (TOut)(object)Convert.ToDouble(value);
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)Convert.ToSingle(value);
#endif
                
                return (TOut)(object)value;
            }
            
            private static TOut UInt64Converter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<ulong>(data);
                

                if (typeof(TOut) == typeof(sbyte))
                    return (TOut)(object)Convert.ToSByte(value);
                if (typeof(TOut) == typeof(short))
                    return (TOut)(object)Convert.ToInt16(value);
                if (typeof(TOut) == typeof(int))
                    return (TOut)(object)Convert.ToInt32(value);
                if (typeof(TOut) == typeof(long))
                    return (TOut)(object)Convert.ToInt64(value);
                
                if (typeof(TOut) == typeof(byte))
                    return (TOut)(object)Convert.ToByte(value);
                if (typeof(TOut) == typeof(ushort))
                    return (TOut)(object)Convert.ToUInt16(value);
                if (typeof(TOut) == typeof(uint))
                    return (TOut)(object)Convert.ToUInt32(value);
                if (typeof(TOut) == typeof(ulong))
                    return (TOut)(object)Convert.ToUInt64(value);
                
                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)Convert.ToSingle(value);
                if (typeof(TOut) == typeof(double))
                    return (TOut)(object)Convert.ToDouble(value);
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)Convert.ToSingle(value);
#endif
                
                return (TOut)(object)value;
            }
            
            private static TOut FloatConverter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<float>(data);

                if (typeof(TOut) == typeof(double))
                    return (TOut)(object)Convert.ToDouble(value);
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)value;
#endif
                return (TOut)(object)value;
            }
            
            private static TOut DoubleConverter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<double>(data);

                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)Convert.ToSingle(value);
                
#if NET6_0_OR_GREATER
                if (typeof(TOut) == typeof(Half))
                    return (TOut)(object)(Half)Convert.ToSingle(value);
#endif
                
                return (TOut)(object)value;
            }

#if NET6_0_OR_GREATER
            private static TOut HalfConverter(byte* data)
            {
                var value = Unsafe.ReadUnaligned<Half>(data);

                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)(float)value;
                
                if (typeof(TOut) == typeof(float))
                    return (TOut)(object)(double)value;
                
                return (TOut)(object)value;
            }
#endif
            
            public void Dispose() { }
        }
    }
}
