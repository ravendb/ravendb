using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Sparrow.Json
{
    public class LazyNumberValue : IComparable, IConvertible
    {
        public readonly LazyStringValue Inner;
        private double? _val;
        private float? _floatVal;
        private decimal? _decimalVal;
        private long? _longVal;
        private ulong? _ulongVal;

        public LazyNumberValue(LazyStringValue inner)
        {
            Inner = inner;
        }

        public static implicit operator long(LazyNumberValue self)
        {
            if (self._longVal != null)
                return self._longVal.Value;

            if (long.TryParse(self.Inner, out var longVal) == false)
            {
                var doubleVal = (double)self;
                longVal = (long)doubleVal;
            }

            self._longVal = longVal;
            return self._longVal.Value; // that's inefficient, but percise
        }

        public static implicit operator ulong(LazyNumberValue self)
        {
            if (self._ulongVal != null)
                return self._ulongVal.Value;

            if (ulong.TryParse(self.Inner, out var ulongVal) == false)
            {
                var doubleVal = (double)self;
                ulongVal = (ulong)doubleVal;
            }

            self._ulongVal = ulongVal;
            return self._ulongVal.Value; // that's inefficient, but percise
        }


        public static implicit operator double(LazyNumberValue self)
        {
            if (self._val != null)
                return self._val.Value;

            var val = double.Parse(self.Inner, NumberStyles.Any, CultureInfo.InvariantCulture);
            self._val = val;
            return val;
        }

        public static implicit operator string(LazyNumberValue self)
        {
            return self.Inner;
        }

        public static implicit operator float(LazyNumberValue self)
        {
            if (self._floatVal != null)
                return (float)self._floatVal;

            var val = float.Parse(self.Inner, NumberStyles.Any, CultureInfo.InvariantCulture);
            self._floatVal = val;
            return val;
        }

        public static implicit operator decimal(LazyNumberValue self)
        {
            if (self._decimalVal != null)
                return self._decimalVal.Value;

            var val = decimal.Parse(self.Inner, NumberStyles.Any, CultureInfo.InvariantCulture);
            self._decimalVal = val;
            return val;
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

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            var lazyDouble = obj as LazyNumberValue;

            if (lazyDouble != null)
                return Equals(lazyDouble);

            if (obj is double)
                return Math.Abs(this - (double)obj) < double.Epsilon;

            if (obj is decimal)
                return ((decimal)this).Equals((decimal)obj);

            if (obj is LazyStringValue l &&
                l.Length == 3) // checking for 3 as optimization
                return Inner.Equals(l); // this is to match NaN

            return false;
        }

        protected bool Equals(LazyNumberValue other)
        {
            if (_val != null && other._val != null)
                return Math.Abs(_val.Value - other._val.Value) < double.Epsilon;

            if (_decimalVal != null && other._decimalVal != null)
                return _decimalVal.Value.Equals(other._decimalVal.Value);

            return Inner.Equals(other.Inner);
        }

        public override int GetHashCode()
        {
            return _val?.GetHashCode() ?? _decimalVal?.GetHashCode() ?? Inner.GetHashCode();
        }

        public int CompareTo(object that)
        {
            if (that is double)
                return Compare(this, (double)that);

            if (that is LazyNumberValue)
                return Compare(this, (LazyNumberValue)that);

            throw new NotSupportedException($"Could not compare with '{that}' of type '{that.GetType()}'.");
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
            if (_val.HasValue && double.IsNaN(_val.Value))
                return true;

            return Inner.Equals("NaN");
        }

        public bool IsPositiveInfinity()
        {
            if (_val.HasValue && double.IsPositiveInfinity(_val.Value))
                return true;

            return Inner.Equals("Infinity");
        }

        public bool IsNegativeInfinity()
        {
            if (_val.HasValue && double.IsNegativeInfinity(_val.Value))
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
