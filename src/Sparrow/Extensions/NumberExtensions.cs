using System;

namespace Sparrow.Extensions
{
    internal static class NumberExtensions
    {
        public static bool AlmostEquals(this double value, double other)
        {
            return HasMinimalDifference(value, other, 1);
        }

        public static bool AlmostEquals(this float value, float other)
        {
            return HasMinimalDifference(value, other, 1);
        }

        /// <summary>
        /// https://docs.microsoft.com/en-us/dotnet/api/system.double.equals?view=net-5.0
        /// </summary>
        private static bool HasMinimalDifference(double value1, double value2, long units)
        {
            long lValue1 = BitConverter.DoubleToInt64Bits(value1);
            long lValue2 = BitConverter.DoubleToInt64Bits(value2);

            // If the signs are different, return false except for +0 and -0.
            if ((lValue1 >> 63) != (lValue2 >> 63))
            {
                if (value1 == value2)
                    return true;

                return false;
            }

            long diff = Math.Abs(lValue1 - lValue2);

            if (diff <= units)
                return true;

            return false;
        }

        /// <summary>
        /// https://docs.microsoft.com/en-us/dotnet/api/system.single.equals?view=net-5.0
        /// </summary>
        private static bool HasMinimalDifference(float value1, float value2, int units)
        {
            byte[] bytes = BitConverter.GetBytes(value1);
            int iValue1 = BitConverter.ToInt32(bytes, 0);

            bytes = BitConverter.GetBytes(value2);
            int iValue2 = BitConverter.ToInt32(bytes, 0);

            // If the signs are different, return false except for +0 and -0.
            if ((iValue1 >> 31) != (iValue2 >> 31))
            {
                if (value1 == value2)
                    return true;

                return false;
            }

            int diff = Math.Abs(iValue1 - iValue2);

            if (diff <= units)
                return true;

            return false;
        }
    }
}
