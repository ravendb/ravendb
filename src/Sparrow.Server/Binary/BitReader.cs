using System;
using System.Runtime.CompilerServices;

namespace Sparrow.Server.Binary
{
    public interface IBitReader
    {
        int Length { get; }
        Bit Read();
    }


    public struct TypedBitReader<T> : IBitReader
        where T : unmanaged
    {
        private int _length;
        private int _shift;
        private readonly T _data;

        public TypedBitReader(T data)
        {
            _length = Unsafe.SizeOf<T>() * 8;
            _data = data;
            _shift = 0;
        }

        public TypedBitReader(T data, int length, int skipped = 0)
        {
            _length = length;
            _data = data;
            _shift = 0;

            if (skipped != 0)
                Skip(skipped);
        }

        public int Length => _length;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int MaskShift()
        {
            if (typeof(long) == typeof(T) || typeof(ulong) == typeof(T))
                return sizeof(long) * 8 - 1;
            if (typeof(int) == typeof(T) || typeof(uint) == typeof(T))
                return sizeof(int) * 8 - 1;
            if (typeof(short) == typeof(T) || typeof(ushort) == typeof(T))
                return sizeof(short) * 8 - 1;
            if (typeof(byte) == typeof(T) || typeof(sbyte) == typeof(T))
                return sizeof(byte) * 8 - 1;

            throw new ArgumentException($"Type '{nameof(T)}' is not supported by this reader.");
        }

        public void Skip(int bits)
        {            
            _shift = (byte)(_shift + bits);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Bit Read()
        {
            if (_length == 0)
                throw new InvalidOperationException("Cannot read from a 0 length stream.");

            // [bit = 0]: xooo_oooo >> 7 [sizeof(byte) - 1 - bit = 7] -> 0000_000x 
            // [bit = 3]: ooox_oooo >> 4 [sizeof(byte) - 1 - 3   = 4] -> 0000_000x 
            // [bit = 7]: oooo_ooox >> 0 [sizeof(byte) - 1 - 7   = 0] -> 0000_000x                

            byte value;
            if (typeof(long) == typeof(T))
            {
                value = (byte)(((ulong)(long)(object)_data) >> (sizeof(ulong) * 8 - 1 - _shift));
            }            
            else if (typeof(ulong) == typeof(T))
            {
                value = (byte)(((ulong)(object)_data) >> (sizeof(ulong) * 8 - 1 - _shift));
            }
            else if (typeof(int) == typeof(T))
            {
                value = (byte)(((uint)(int)(object)_data) >> (sizeof(uint) * 8 - 1 - _shift));
            }
            else if (typeof(uint) == typeof(T))
            {
                value = (byte)(((uint)(object)_data) >> (sizeof(uint) * 8 - 1 - _shift));
            }
            else if (typeof(short) == typeof(T))
            {
                value = (byte)(((ushort)(short)(object)_data) >> (sizeof(ushort) * 8 - 1 - _shift));
            }
            else if (typeof(ushort) == typeof(T))
            {
                value = (byte)(((ushort)(object)_data) >> (sizeof(ushort) * 8 - 1 - _shift));
            }
            else if (typeof(sbyte) == typeof(T))
            {
                value = (byte)(((byte)(sbyte)(object)_data) >> (sizeof(byte) * 8 - 1 - _shift));
            }
            else if (typeof(byte) == typeof(T))
            {
                value = (byte)(((byte)(object)_data) >> (sizeof(byte) * 8 - 1 - _shift));
            }
            else
            {
                throw new ArgumentException($"Type '{nameof(T)}' is not supported by this reader.");
            }
                
            _shift++;
            _length--;
            return new Bit(value);                        
        }
    }

    public ref struct BitReader
    {
        private int _length;
        private int _shift;
        private ReadOnlySpan<byte> _data;

        public BitReader(ReadOnlySpan<byte> data)
        {
            _length = data.Length * 8;
            _data = data;
            _shift = 0;
        }

        public BitReader(ReadOnlySpan<byte> data, int length, int skipped = 0)
        {
            _length = length;
            _data = data;
            _shift = 0;

            if (skipped != 0)
                Skip(skipped);
        }

        public int Length => _length;

        public void Skip(int bits)
        {
            int bytesSkipped = (_shift + bits) / 8;

            _data = _data.Slice(bytesSkipped);
            _shift = (byte)((_shift + bits) % 8);
            _length -= bits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Bit Read()
        {
            if (_length == 0)
                throw new InvalidOperationException("Cannot read from a 0 length stream.");

            int value = _data[0] >> (7 - _shift);
            _length--;
            _shift++;

            if (_shift >= 8) // Will wrap around. 
            {
                _data = _data.Slice(1);
                _shift = 0;
            }

            return new Bit((byte)value);
        }
    }
}
