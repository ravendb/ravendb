using System;
using System.Buffers.Text;
using System.Runtime.CompilerServices;
using Sparrow.Extensions;

namespace Sparrow.Json
{
    public sealed class LazyNumberValue : IComparable, IConvertible
    {
        public readonly LazyStringValue Inner;

        // results of the conversions, valid only when the matching flag is set in _cached
        // (plain fields and a single flags byte instead of nullables, to keep the instance small)
        private double _val;
        private float _floatVal;
        private decimal _decimalVal;
        private long _longVal;
        private ulong _ulongVal;
        private CachedValues _cached;

        [Flags]
        private enum CachedValues : byte
        {
            None = 0,
            Double = 1,
            Float = 2,
            Decimal = 4,
            Long = 8,
            ULong = 16
        }

        public LazyNumberValue(LazyStringValue inner)
        {
            Inner = inner;
        }

        private bool HasDouble => (_cached & CachedValues.Double) != 0;

        private bool HasDecimal => (_cached & CachedValues.Decimal) != 0;

        public static implicit operator long(LazyNumberValue self)
        {
            if ((self._cached & CachedValues.Long) != 0)
                return self._longVal;

            if (self.TryParseLong(out var val) == false)
            {
                var doubleVal = (double)self;
                val = (long)doubleVal;
            }

            self._longVal = val;
            self._cached |= CachedValues.Long;
            return val;
        }

        public static implicit operator ulong(LazyNumberValue self)
        {
            if ((self._cached & CachedValues.ULong) != 0)
                return self._ulongVal;

            if (self.TryParseULong(out var val) == false)
            {
                var doubleVal = (double)self;
                val = (ulong)doubleVal;
            }

            self._ulongVal = val;
            self._cached |= CachedValues.ULong;
            return val;
        }

        public static implicit operator double(LazyNumberValue self)
        {
            if (self.HasDouble)
                return self._val;

            if (self.TryParseDouble(out var val) == false)
            {
                ThrowInvalidNumberFormat(self, "double");
            }

            self._val = val;
            self._cached |= CachedValues.Double;
            return val;
        }

        public static implicit operator string(LazyNumberValue self)
        {
            return self.Inner;
        }

        public static implicit operator float(LazyNumberValue self)
        {
            if ((self._cached & CachedValues.Float) != 0)
                return self._floatVal;

            if (self.TryParseFloat(out var val) == false)
            {
                var doubleVal = (double)self;
                val = (float)doubleVal;
            }

            self._floatVal = val;
            self._cached |= CachedValues.Float;
            return val;
        }

        public static implicit operator decimal(LazyNumberValue self)
        {
            if (self.HasDecimal)
                return self._decimalVal;

            if (self.TryParseDecimal(out var val) == false)
            {
                ThrowInvalidNumberFormat(self, "decimal");
            }

            self._decimalVal = val;
            self._cached |= CachedValues.Decimal;
            return val;
        }

        private static void ThrowInvalidNumberFormat(LazyNumberValue self, string type)
        {
            throw new InvalidCastException($"Unable to convert '{self.Inner}' to a {type}");
        }

        public static decimal operator *(LazyNumberValue x, LazyNumberValue y)
        {
            return (decimal)x * (decimal)y;
        }

        public static decimal operator /(LazyNumberValue x, LazyNumberValue y)
        {
            return (decimal)x / (decimal)y;
        }

        public static decimal operator +(LazyNumberValue x, LazyNumberValue y)
        {
            return (decimal)x + (decimal)y;
        }

        public static decimal operator -(LazyNumberValue x, LazyNumberValue y)
        {
            return (decimal)x - (decimal)y;
        }

        public static decimal operator %(LazyNumberValue x, LazyNumberValue y)
        {
            return (decimal)x % (decimal)y;
        }

        public static decimal operator *(long x, LazyNumberValue y)
        {
            return x * (decimal)y;
        }

        public static decimal operator /(long x, LazyNumberValue y)
        {
            return x / (decimal)y;
        }

        public static decimal operator +(long x, LazyNumberValue y)
        {
            return x + (decimal)y;
        }

        public static decimal operator -(long x, LazyNumberValue y)
        {
            return x - (decimal)y;
        }

        public static decimal operator %(long x, LazyNumberValue y)
        {
            return x % (decimal)y;
        }
        public static decimal operator +(LazyNumberValue y)
        {
            return (decimal)y;
        }

        public static decimal operator -(LazyNumberValue y)
        {
            return -(decimal)y;
        }

        public static bool operator !=(LazyNumberValue self, LazyNumberValue lnv) => !(self == lnv);
        
        public static bool operator ==(LazyNumberValue self, LazyNumberValue lnv)
        {
            if (ReferenceEquals(self, null))
                return ReferenceEquals(lnv, null);
            
            if (ReferenceEquals(lnv, null))
                return false;

            return self.Equals(lnv);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            if (obj is LazyNumberValue lazyDouble)
                return Equals(lazyDouble);

            if (obj is double dbl)
                return dbl.AlmostEquals(this);

            if (obj is decimal dec)
                return ((decimal)this).Equals(dec);

            if (obj is LazyStringValue l &&
                l.Length == 3) // checking for 3 as optimization
                return Inner.Equals(l); // this is to match NaN

            return false;
        }

        private bool Equals(LazyNumberValue other)
        {
            if (HasDouble && other.HasDouble)
                return _val.AlmostEquals(other._val);

            if (HasDecimal && other.HasDecimal)
                return _decimalVal.Equals(other._decimalVal);

            return Inner.Equals(other.Inner);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool TryParseLong(out long longValue)
        {
            return Utf8Parser.TryParse(new ReadOnlySpan<byte>(Inner.Buffer, Inner.Size), out longValue, out var consumed) &&
                   Inner.Size == consumed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool TryParseULong(out ulong ulongValue)
        {
            return Utf8Parser.TryParse(new ReadOnlySpan<byte>(Inner.Buffer, Inner.Size), out ulongValue, out var consumed) &&
                   Inner.Size == consumed;
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool TryParseDouble(out double doubleVal)
        {
            return Utf8Parser.TryParse(new ReadOnlySpan<byte>(Inner.Buffer, Inner.Size), out doubleVal, out var consumed) &&
                   Inner.Size == consumed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool TryParseFloat(out float floatVal)
        {
            return Utf8Parser.TryParse(new ReadOnlySpan<byte>(Inner.Buffer, Inner.Size), out floatVal, out var consumed) &&
                   Inner.Size == consumed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool TryParseDecimal(out decimal decimalValue)
        {
            return Utf8Parser.TryParse(new ReadOnlySpan<byte>(Inner.Buffer, Inner.Size), out decimalValue, out var consumed) &&
                Inner.Size == consumed;
        }

        public override int GetHashCode()
        {
            if (HasDouble)
                return _val.GetHashCode();

            if (HasDecimal)
                return _decimalVal.GetHashCode();

            return Inner.GetHashCode();
        }

        public int CompareTo(object that)
        {
            if (that is double d)
                return Compare(this, d);

            if (that is long l)
                return Compare(this, l);

            if (that is LazyNumberValue lnv)
                return Compare(this, lnv);

            throw new NotSupportedException($"Could not compare with '{that}' of type '{that.GetType()}'.");
        }

        public int? TryCompareTo(object that)
        {
            if (that is double d)
                return Compare(this, d);

            if (that is long l)
                return Compare(this, l);

            if (that is LazyNumberValue lnv)
                return Compare(this, lnv);

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int Compare(double @this, double that)
        {
            if (@this > that)
                return 1;

            if (@this < that)
                return -1;

            return 0;
        }

        public override string ToString()
        {
            return Inner.ToString();
        }

        public string ToString(string format)
        {
            var @double = (double)this;
            return @double.ToString(format);
        }

        public bool IsNaN()
        {
            if (HasDouble && double.IsNaN(_val))
                return true;

            return Inner.Equals("NaN");
        }

        public bool IsPositiveInfinity()
        {
            if (HasDouble && double.IsPositiveInfinity(_val))
                return true;

            return Inner.Equals("Infinity");
        }

        public bool IsNegativeInfinity()
        {
            if (HasDouble && double.IsNegativeInfinity(_val))
                return true;

            return Inner.Equals("-Infinity");
        }

        public TypeCode GetTypeCode()
        {
            return TypeCode.Object;
        }

        private void ThrowInvalidCaseException(string typeName)
        {
            throw new InvalidCastException($"Could not cast {nameof(LazyNumberValue)} to {typeName}");
        }

        public bool ToBoolean(IFormatProvider provider)
        {
            var asString = (string)Inner;
            if (asString == "0")
                return true;
            else if (asString == "1")
                return false;

            ThrowInvalidCaseException("boolean");
            return false;
        }

        public byte ToByte(IFormatProvider provider)
        {
            return (byte)(double)this;
        }

        public char ToChar(IFormatProvider provider)
        {
            return (char)(double)this;
        }

        public DateTime ToDateTime(IFormatProvider provider)
        {
            return new DateTime((long)this);
        }

        public decimal ToDecimal(IFormatProvider provider)
        {
            return this;
        }

        public double ToDouble(IFormatProvider provider)
        {
            return this;
        }

        public short ToInt16(IFormatProvider provider)
        {
            return (short)(double)this;
        }

        public int ToInt32(IFormatProvider provider)
        {
            return (int)(double)this;
        }

        public long ToInt64(IFormatProvider provider)
        {
            return this;
        }

        public sbyte ToSByte(IFormatProvider provider)
        {
            return (sbyte)(double)this;
        }

        public float ToSingle(IFormatProvider provider)
        {
            return (float)(double)this;
        }

        public string ToString(IFormatProvider provider)
        {
            return Inner;
        }

        public object ToType(Type conversionType, IFormatProvider provider)
        {
            return typeof(LazyNumberValue);
        }

        public ushort ToUInt16(IFormatProvider provider)
        {
            return (ushort)(double)this;
        }

        public uint ToUInt32(IFormatProvider provider)
        {
            return (uint)(double)this;
        }

        public ulong ToUInt64(IFormatProvider provider)
        {
            return this;
        }
    }
}
