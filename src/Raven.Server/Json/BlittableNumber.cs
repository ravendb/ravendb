using System;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Json
{
    public static class BlittableNumber
    {
        public static NumberParseResult Parse(object value, out double doubleResult, out long longResult)
        {
            if (value is long || value is int)
            {
                longResult = Convert.ToInt64(value);
                doubleResult = double.MinValue;

                return NumberParseResult.Long;
            }

            if (value is double)
            {
                longResult = long.MinValue;
                doubleResult = (double)value;

                return NumberParseResult.Double;
            }

            if (value is decimal)
            {
                var d = (decimal)value;
                if (DecimalHelper.Instance.IsDouble(ref d))
                {
                    doubleResult = (double)d;
                    longResult = long.MinValue;

                    return NumberParseResult.Double;
                }

                longResult = (long)d;
                doubleResult = double.MinValue;

                return NumberParseResult.Long;
            }

            var lazyDouble = value as LazyDoubleValue;
            if (lazyDouble != null)
            {
                doubleResult = lazyDouble;
                longResult = long.MinValue;

                return NumberParseResult.Double;
            }

            throw new InvalidOperationException($"Could not parse numeric field for the value '{value}' of the given type: {value.GetType().FullName}");
        }
    }

    public enum NumberParseResult
    {
        Double,
        Long
    }
}