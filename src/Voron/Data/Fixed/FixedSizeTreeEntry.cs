using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Voron.Data.Fixed
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct FixedSizeTreeEntry
    {
        [FieldOffset(0)]
        private long _key;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TVal GetKey<TVal>()
            where TVal : unmanaged, IBinaryNumber<TVal>
        {
            if (typeof(TVal) == typeof(long))
                return (TVal)(object)_key;
            if (typeof(TVal) == typeof(double))
                return (TVal)(object)BitConverter.Int64BitsToDouble(_key);
            throw new NotSupportedException("Unknown type: " + typeof(TVal));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetKey<TVal>(TVal value)
            where TVal : unmanaged, IBinaryNumber<TVal>
        {
            if (typeof(TVal) == typeof(long))
                _key = (long)(object)value;
            else if (typeof(TVal) == typeof(double))
                _key = BitConverter.DoubleToInt64Bits((double)(object)value);
            else
                throw new NotSupportedException("Unknown type: " + typeof(TVal));
        }

        [FieldOffset(8)]
        public byte* Value;

        [FieldOffset(8)]
        public long PageNumber;
    }
}
