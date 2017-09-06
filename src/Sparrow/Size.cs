// -----------------------------------------------------------------------
//  <copyright file="Size.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;

namespace Sparrow
{
    public struct Size
    {
        public static readonly Type TypeOf = typeof(Size);
        public static readonly Type NullableTypeOf = typeof(Size?);

        public static readonly Size Zero = new Size(0, SizeUnit.Bytes);

        private const long OneKb = 1024;
        private const long OneMb = OneKb * 1024;
        private const long OneGb = OneMb * 1024;
        private const long OneTb = OneGb * 1024;

        private readonly SizeUnit _unit;
        private long _valueInBytes;

        public Size(long value, SizeUnit unit)
        {
            _unit = unit;
            _valueInBytes = ConvertToBytes(value, unit);
        }

        private static long ConvertToBytes(long value, SizeUnit unit)
        {
            switch (unit)
            {
                case SizeUnit.Bytes:
                    return value;
                case SizeUnit.Kilobytes:
                    return value * OneKb;
                case SizeUnit.Megabytes:
                    return value * OneMb;
                case SizeUnit.Gigabytes:
                    return value * OneGb;
                case SizeUnit.Terabytes:
                    return value * OneTb;
                default:
                    throw new NotSupportedException("Not supported size unit: " + unit);
            }
        }

        public static double ConvertToBytes(double value, SizeUnit unit)
        {
            switch (unit)
            {
                case SizeUnit.Bytes:
                    return value;
                case SizeUnit.Kilobytes:
                    return value * OneKb;
                case SizeUnit.Megabytes:
                    return value * OneMb;
                case SizeUnit.Gigabytes:
                    return value * OneGb;
                case SizeUnit.Terabytes:
                    return value * OneTb;
                default:
                    throw new NotSupportedException("Not supported size unit: " + unit);
            }
        }

        [Pure]
        public long GetValue(SizeUnit requestedUnit)
        {
            switch (requestedUnit)
            {
                case SizeUnit.Bytes:
                    return _valueInBytes;
                case SizeUnit.Kilobytes:
                    return _valueInBytes / OneKb;
                case SizeUnit.Megabytes:
                    return _valueInBytes / OneMb;
                case SizeUnit.Gigabytes:
                    return _valueInBytes / OneGb;
                case SizeUnit.Terabytes:
                    return _valueInBytes / OneTb;
                default:
                    ThrowUnsupportedSize();
                    return -1;// never hit
            }
        }

        [Pure]
        public double GetDoubleValue(SizeUnit requestedUnit)
        {
            switch (requestedUnit)
            {
                case SizeUnit.Bytes:
                    return _valueInBytes;
                case SizeUnit.Kilobytes:
                    return _valueInBytes / (double)OneKb;
                case SizeUnit.Megabytes:
                    return _valueInBytes / (double)OneMb;
                case SizeUnit.Gigabytes:
                    return _valueInBytes / (double)OneGb;
                case SizeUnit.Terabytes:
                    return _valueInBytes / (double)OneTb;
                default:
                    ThrowUnsupportedSize();
                    return -1;// never hit
            }
        }

        private void ThrowUnsupportedSize()
        {
            throw new NotSupportedException("Not supported size unit: " + _unit);
        }

        public void Add(int value, SizeUnit unit)
        {
            _valueInBytes += ConvertToBytes(value, unit);
        }

        public void Add(long value, SizeUnit unit)
        {
            _valueInBytes += ConvertToBytes(value, unit);
        }

        public static bool operator <(Size x, Size y)
        {
            return x._valueInBytes < y._valueInBytes;
        }

        public static bool operator >(Size x, Size y)
        {
            return x._valueInBytes > y._valueInBytes;
        }

        public static bool operator <=(Size x, Size y)
        {
            return x._valueInBytes <= y._valueInBytes;
        }

        public static bool operator >=(Size x, Size y)
        {
            return x._valueInBytes >= y._valueInBytes;
        }

        public static Size operator +(Size x, Size y)
        {
            return new Size(x._valueInBytes + y._valueInBytes, SizeUnit.Bytes);
        }

        public static Size operator -(Size x, Size y)
        {
            return new Size(x._valueInBytes - y._valueInBytes, SizeUnit.Bytes);
        }

        public static Size operator *(Size x, long y)
        {
            return new Size(x._valueInBytes * y, SizeUnit.Bytes);
        }

        public static Size operator *(Size x, double y)
        {
            return new Size((long)(x._valueInBytes * y), SizeUnit.Bytes);
        }

        public static Size operator *(double y, Size x)
        {
            return new Size((long)(x._valueInBytes * y), SizeUnit.Bytes);
        }

        public static Size operator /(Size x, int y)
        {
            return new Size(x._valueInBytes / y, SizeUnit.Bytes);
        }

        public static Size Min(Size x, Size y)
        {
            return x._valueInBytes < y._valueInBytes ? x : y;
        }

        public static Size Max(Size x, Size y)
        {
            return x._valueInBytes > y._valueInBytes ? x : y;
        }

        public static Size Sum(ICollection<Size> sizes)
        {
            return new Size(sizes.Sum(x => x._valueInBytes), SizeUnit.Bytes);
        }

        public override string ToString()
        {
            if (Math.Abs(_valueInBytes) > OneTb)
                return $"{Math.Round(_valueInBytes / (double)OneTb, 4):#,#.####} TBytes";
            if (Math.Abs(_valueInBytes) > OneGb)
                return $"{Math.Round(_valueInBytes / (double)OneGb, 3):#,#.###} GBytes";
            if (Math.Abs(_valueInBytes) > OneMb)
                return $"{Math.Round(_valueInBytes / (double)OneMb, 2):#,#.##} MBytes";
            if (Math.Abs(_valueInBytes) > OneKb)
                return $"{Math.Round(_valueInBytes / (double)OneKb, 2):#,#.##} KBytes";
            return $"{_valueInBytes:#,#} Bytes";
        }
    }

    public enum SizeUnit
    {
        Bytes,
        Kilobytes,
        Megabytes,
        Gigabytes,
        Terabytes
    }
}
