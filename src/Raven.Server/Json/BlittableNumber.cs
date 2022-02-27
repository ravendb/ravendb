using System;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Json
{
    public static class BlittableNumber
    {
        public static NumberParseResult Parse(object value, out double doubleResult, out long longResult)
        {
            if (value is long || value is int || value is short || value is byte || value is ulong || value is uint || value is ushort || value is sbyte)
            {
                longResult = Convert.ToInt64(value);
                doubleResult = double.MinValue;

                return NumberParseResult.Long;
            }

            if (value is double dbl)
            {
                // ReSharper disable once CompareOfFloatsByEqualityOperator
                if (dbl == 0.0)
                {
                    // this is NOT superfluous code, we need 
                    // to handle negative zeros
                    //https://blogs.msdn.microsoft.com/bclteam/2006/10/12/decimal-negative-zero-representation-lakshan-fernando/
                    dbl = 0.0D;
                }

                longResult = long.MinValue;
                doubleResult = dbl;

                return NumberParseResult.Double;
            }

            if (value is float f)
            {
                // ReSharper disable once CompareOfFloatsByEqualityOperator
                if (f == 0.0)
                {
                    // this is NOT superfluous code, we need 
                    // to handle negative zeros
                    //https://blogs.msdn.microsoft.com/bclteam/2006/10/12/decimal-negative-zero-representation-lakshan-fernando/
                    f = 0.0F;
                }

                longResult = long.MinValue;
                doubleResult = (double)(decimal)f;

                return NumberParseResult.Double;
            }

            if (value is decimal d)
            {
                if (DecimalHelper.Instance.IsDouble(ref d) || d > long.MaxValue || d < long.MinValue)
                {
                    if (d == 0)
                    {
                        // this is NOT superfluous code, we need 
                        // to handle negative zeros
                        //https://blogs.msdn.microsoft.com/bclteam/2006/10/12/decimal-negative-zero-representation-lakshan-fernando/
                        d = decimal.Zero;
                    }

                    doubleResult = (double)d;
                    longResult = long.MinValue;

                    return NumberParseResult.Double;
                }

                longResult = (long)d;
                doubleResult = double.MinValue;

                return NumberParseResult.Long;
            }

            if (value is LazyNumberValue lazyDouble)
            {
                doubleResult = lazyDouble;
                longResult = long.MinValue;

                return NumberParseResult.Double;
            }

            throw new InvalidOperationException($"Could not parse numeric field for the value '{value ?? "null" }' of the given type: {value?.GetType().FullName ?? "null value"}");
        }
    }

    public enum NumberParseResult
    {
        Double,
        Long
    }
}
