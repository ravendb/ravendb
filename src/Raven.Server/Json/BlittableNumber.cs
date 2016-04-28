using System;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class BlittableNumber
    {
        public static NumberParseResult Parse(object value, LazyStringReader reader, out double doubleResult, out long longResult)
        {
            if (value is long)
            {
                longResult = (long)value;
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