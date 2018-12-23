using System;
using System.Globalization;
using Raven.Client.Documents.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static.Extensions
{
    public static class DynamicExtensionMethods
    {
        public static BoostedValue Boost(dynamic o, object value)
        {
            return new BoostedValue
            {
                Value = o,
                Boost = Convert.ToSingle(value)
            };
        }

        public static dynamic ParseInt(dynamic o)
        {
            return ParseInt(o, default(int));
        }

        public static dynamic ParseInt(dynamic o, int defaultValue)
        {
            if (o == null)
                return defaultValue;

            if (TryGetString(o, out string value) == false)
                return defaultValue;

            return int.TryParse(value, out int result) ? result : defaultValue;
        }

        public static dynamic ParseDouble(dynamic o)
        {
            return ParseDouble(o, default(double));
        }

        public static dynamic ParseDouble(dynamic o, double defaultValue)
        {
            if (o == null)
                return defaultValue;

            if (TryGetString(o, out string value) == false)
                return defaultValue;

            return double.TryParse(value, out double result) ? result : defaultValue;
        }

        public static dynamic ParseDecimal(dynamic o)
        {
            return ParseDecimal(o, default(decimal));
        }

        public static dynamic ParseDecimal(dynamic o, decimal defaultValue)
        {
            if (o == null)
                return defaultValue;

            if (TryGetString(o, out string value) == false)
                return defaultValue;

            return decimal.TryParse(value, out decimal result) ? result : defaultValue;
        }

        public static dynamic ParseLong(dynamic o)
        {
            return ParseLong(o, default(long));
        }

        public static dynamic ParseLong(dynamic o, long defaultValue)
        {
            if (o == null)
                return defaultValue;

            if (TryGetString(o, out string value) == false)
                return defaultValue;

            return long.TryParse(value, out long result) ? result : defaultValue;
        }

        private static bool TryGetString(dynamic o, out string value)
        {
            var stringValue = o as string;
            if (stringValue != null)
            {
                value = stringValue;
                return true;
            }

            var lsv = o as LazyStringValue;
            if (lsv == null)
            {
                var lcsv = o as LazyCompressedStringValue;
                if (lcsv != null)
                    lsv = lcsv.ToLazyStringValue();
            }

            if (lsv != null)
            {
                value = lsv.ToString();
                return true;
            }

            value = null;
            return false;
        }

        public static string Substring(dynamic o, int value)
        {
            if (o is IConvertible c)
            {
                string result = c.ToString(CultureInfo.InvariantCulture).Substring(value);
                return result;
            }

            return o.ToString().Substring(value);
        }

        public static string Substring(dynamic o, int from, int to)
        {
            if (o is IConvertible c)
            {
                var result = c.ToString(CultureInfo.InvariantCulture).Substring(from, to);
                return result;
            }

            return o.ToString().Substring(from, to);
        }
        
        public static int IndexOf(dynamic o, char value)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).IndexOf(value);

            return o.ToString().IndexOf(value);
        }

        public static int IndexOf(dynamic o, char value, StringComparison comparisonType)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).IndexOf(value, comparisonType);

            return o.ToString().IndexOf(value, comparisonType);
        }

        public static int IndexOf(dynamic o, char value, int startIndex)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).IndexOf(value, startIndex);

            return o.ToString().IndexOf(value, startIndex);
        }

        public static int IndexOf(dynamic o, char value, int startIndex, int count)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).IndexOf(value, startIndex, count);

            return o.ToString().IndexOf(value, startIndex, count);
        }

        public static int IndexOf(dynamic o, string value)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).IndexOf(value, StringComparison.InvariantCulture);

            return o.ToString().IndexOf(value);
        }

        public static bool StartsWith(dynamic o, char value)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).StartsWith(value);

            return o.ToString().StartsWith(value);
        }

        public static bool StartsWith(dynamic o, string value)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).StartsWith(value);

            return o.ToString().StartsWith(value);
        }

        public static bool StartsWith(dynamic o, string value, StringComparison comparisonType)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).StartsWith(value, comparisonType);

            return o.ToString().StartsWith(value, comparisonType);
        }

        public static bool StartsWith(dynamic o, string value, bool ignoreCase, CultureInfo cultureInfo)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).StartsWith(value, ignoreCase, cultureInfo);

            return o.ToString().StartsWith(value, ignoreCase, cultureInfo);
        }

        public static bool EndsWith(dynamic o, char value)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).EndsWith(value);

            return o.ToString().EndsWith(value);
        }

        public static bool EndsWith(dynamic o, string value)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).EndsWith(value);

            return o.ToString().EndsWith(value);
        }

        public static bool EndsWith(dynamic o, string value, StringComparison comparisonType)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).EndsWith(value, comparisonType);

            return o.ToString().EndsWith(value, comparisonType);
        }

        public static bool EndsWith(dynamic o, string value, bool ignoreCase, CultureInfo cultureInfo)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).EndsWith(value, ignoreCase, cultureInfo);

            return o.ToString().EndsWith(value, ignoreCase, cultureInfo);
        }

        public static bool Contains(dynamic o, char value)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).Contains(value);

            return o.ToString().Contains(value);
        }

        public static bool Contains(dynamic o, char value, StringComparison comparisonType)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).Contains(value, comparisonType);

            return o.ToString().Contains(value, comparisonType);
        }

        public static bool Contains(dynamic o, string value)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).Contains(value);

            return o.ToString().Contains(value);
        }

        public static bool Contains(dynamic o, string value, StringComparison comparisonType)
        {
            if (o is IConvertible c)
                return c.ToString(CultureInfo.InvariantCulture).Contains(value, comparisonType);

            return o.ToString().Contains(value, comparisonType);
        }
    }
}
