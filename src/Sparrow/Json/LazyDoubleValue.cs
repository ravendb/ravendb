using System;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Sparrow.Json
{
    public class LazyDoubleValue : IComparable
    {
        public readonly LazyStringValue Inner;
        private double? _val;
        private decimal? _decimalVal;
        private long? _longVal;

        public LazyDoubleValue(LazyStringValue inner)
        {
            Inner = inner;
        }

        public static implicit operator long(LazyDoubleValue self)
        {
            if (self._longVal != null)
                return self._longVal.Value;

            var doubleVal = (double)self;
            var val = (long)doubleVal;
            self._longVal = val;

            return val;
        }

        public static implicit operator double(LazyDoubleValue self)
        {
            if (self._val != null)
                return self._val.Value;

            var val = double.Parse((string)self.Inner, NumberStyles.Any, CultureInfo.InvariantCulture);
            self._val = val;
            return val;
        }

        public static implicit operator decimal(LazyDoubleValue self)
        {
            if (self._decimalVal != null)
                return self._decimalVal.Value;

            var val = decimal.Parse((string)self.Inner, NumberStyles.Any, CultureInfo.InvariantCulture);
            self._decimalVal = val;
            return val;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
                return false;

            if (ReferenceEquals(this, obj))
                return true;

            var lazyDouble = obj as LazyDoubleValue;

            if (lazyDouble != null)
                return Equals(lazyDouble);

            if (obj is double)
                return Math.Abs(this - (double)obj) < double.Epsilon;

            if (obj is decimal)
                return ((decimal)this).Equals((decimal)obj);

            return false;
        }

        protected bool Equals(LazyDoubleValue other)
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

            if (that is LazyDoubleValue)
                return Compare(this, (LazyDoubleValue)that);

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
    }
}