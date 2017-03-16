using System;
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

            string value;
            if (TryGetString(o, out value) == false)
                return defaultValue;

            int result;
            return int.TryParse(value, out result) ? result : defaultValue;
        }

        public static dynamic ParseDouble(dynamic o)
        {
            return ParseDouble(o, default(double));
        }

        public static dynamic ParseDouble(dynamic o, double defaultValue)
        {
            if (o == null)
                return defaultValue;

            string value;
            if (TryGetString(o, out value) == false)
                return defaultValue;

            double result;
            return double.TryParse(value, out result) ? result : defaultValue;
        }

        public static dynamic ParseDecimal(dynamic o)
        {
            return ParseDecimal(o, default(decimal));
        }

        public static dynamic ParseDecimal(dynamic o, decimal defaultValue)
        {
            if (o == null)
                return defaultValue;

            string value;
            if (TryGetString(o, out value) == false)
                return defaultValue;

            decimal result;
            return decimal.TryParse(value, out result) ? result : defaultValue;
        }

        public static dynamic ParseLong(dynamic o)
        {
            return ParseLong(o, default(long));
        }

        public static dynamic ParseLong(dynamic o, long defaultValue)
        {
            if (o == null)
                return defaultValue;

            string value;
            if (TryGetString(o, out value) == false)
                return defaultValue;

            long result;
            return long.TryParse(value, out result) ? result : defaultValue;
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
    }
}