// -----------------------------------------------------------------------
//  <copyright file="Size.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Config.Settings
{
    public struct Size
    {
        public static readonly Type TypeOf = typeof (Size);
        public static readonly Type NullableTypeOf = typeof (Size?);

        private const long OneKb = 1024;
        private const long OneMb = OneKb * 1024;
        private const long OneGb = OneMb * 1024;
        private const long OneTb = OneGb * 1024;

        private readonly SizeUnit unit;
        private readonly long value;
        private readonly long valueInBytes;

        public Size(long value, SizeUnit unit)
        {
            this.value = value;
            this.unit = unit;

            switch (unit)
            {
                case SizeUnit.Bytes:
                    valueInBytes = value;
                    break;
                case SizeUnit.Kilobytes:
                    valueInBytes = value * OneKb;
                    break;
                case SizeUnit.Megabytes:
                    valueInBytes = value * OneMb;
                    break;
                case SizeUnit.Gigabytes:
                    valueInBytes = value * OneGb;
                    break;
                case SizeUnit.Terabytes:
                    valueInBytes = value * OneTb;
                    break;
                default:
                    throw new NotSupportedException("Not supported size unit: " + unit);
            }
        }

        public long GetValue(SizeUnit requestedUnit)
        {
            switch (requestedUnit)
            {
                case SizeUnit.Bytes:
                    return valueInBytes;
                case SizeUnit.Kilobytes:
                    return valueInBytes / OneKb;
                case SizeUnit.Megabytes:
                    return valueInBytes / OneMb;
                case SizeUnit.Gigabytes:
                    return valueInBytes / OneGb;
                case SizeUnit.Terabytes:
                    return valueInBytes / OneTb;
                default:
                    throw new NotSupportedException("Not supported size unit: " + unit);
            }
        }

        public static bool operator <(Size x, Size y)
        {
            return x.valueInBytes < y.valueInBytes;
        }

        public static bool operator >(Size x, Size y)
        {
            return x.valueInBytes > y.valueInBytes;
        }

        public static bool operator <=(Size x, Size y)
        {
            return x.valueInBytes <= y.valueInBytes;
        }

        public static bool operator >=(Size x, Size y)
        {
            return x.valueInBytes >= y.valueInBytes;
        }

        public static Size operator +(Size x, Size y)
        {
            return new Size(x.valueInBytes + y.valueInBytes, SizeUnit.Bytes);
        }

        public static Size operator -(Size x, Size y)
        {
            return new Size(x.valueInBytes - y.valueInBytes, SizeUnit.Bytes);
        }

        public static Size operator *(Size x, long y)
        {
            return new Size(x.valueInBytes * y, SizeUnit.Bytes);
        }

        public static Size operator *(Size x, double y)
        {
            return new Size((long)(x.valueInBytes * y), SizeUnit.Bytes);
        }

        public static Size operator *(double y, Size x)
        {
            return new Size((long)(x.valueInBytes * y), SizeUnit.Bytes);
        }

        public static Size operator /(Size x, int y)
        {
            return new Size(x.valueInBytes / y, SizeUnit.Bytes);
        }

        public static Size Min(Size x, Size y)
        {
            return x.valueInBytes < y.valueInBytes ? x : y;
        }

        public static Size Max(Size x, Size y)
        {
            return x.valueInBytes > y.valueInBytes ? x : y;
        }

        public static Size Sum(ICollection<Size> sizes)
        {
            return new Size(sizes.Sum(x => x.valueInBytes), SizeUnit.Bytes);
        }

        public override string ToString()
        {
            if (valueInBytes > OneTb)
                return $"{valueInBytes / OneTb:#,#.##} TBytes";
            if (valueInBytes > OneGb)
                return $"{valueInBytes / OneGb:#,#.##} GBytes";
            if (valueInBytes > OneMb)
                return $"{valueInBytes / OneMb:#,#.##} MBytes";
            if (valueInBytes > OneKb)
                return $"{valueInBytes / OneKb:#,#.##} KBytes";
            return $"{valueInBytes:#,#} Bytes";
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
