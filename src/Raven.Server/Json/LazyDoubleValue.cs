using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Raven.Server.Json
{
    public class LazyDoubleValue : IConvertible
    {
        public readonly LazyStringValue Inner;
        private double? _val;

        private double Val => _val.GetValueOrDefault();

        public LazyDoubleValue(LazyStringValue inner)
        {
            Inner = inner;
        }

        public static implicit operator double(LazyDoubleValue self)
        {
            if (self._val != null)
                return self._val.Value;

            var val = double.Parse(self.Inner, CultureInfo.InvariantCulture);
            self._val = val;
            return val;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TypeCode GetTypeCode()
        {
            return TypeCode.Double;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ToBoolean(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToBoolean(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte ToByte(IFormatProvider provider)
        {
            return ((IConvertible)Val).ToByte(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public char ToChar(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToChar(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public DateTime ToDateTime(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToDateTime(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public decimal ToDecimal(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToDecimal(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double ToDouble(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToDouble(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public short ToInt16(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToInt16(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int ToInt32(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToInt32(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ToInt64(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToInt64(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sbyte ToSByte(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToSByte(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float ToSingle(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToSingle(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string ToString(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToString(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public object ToType(Type conversionType, IFormatProvider provider)
        {
            return ((IConvertible) Val).ToType(conversionType,provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ushort ToUInt16(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToUInt16(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public uint ToUInt32(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToUInt32(provider);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ulong ToUInt64(IFormatProvider provider)
        {
            return ((IConvertible) Val).ToUInt64(provider);
        }
    }
}