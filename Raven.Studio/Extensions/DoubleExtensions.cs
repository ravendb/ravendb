using System;

namespace Raven.Studio.Extensions
{
    public static class DoubleExtensions
    {
        public static bool IsCloseTo(this double value, double other, double epsilon = 0.0000001)
        {
            return Math.Abs(value - other) < epsilon;
        }
    }
}