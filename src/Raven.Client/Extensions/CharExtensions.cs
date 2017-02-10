using System;
using System.Globalization;

namespace Raven.Client.Extensions
{
    public static class CharExtensions
    {
        public static string CharToString(this char c)
        {
            return c.ToString();
        }

        public static string ToInvariantString(this object obj)
        {
            return obj is IConvertible ? ((IConvertible)obj).ToString(CultureInfo.InvariantCulture)
                : obj is IFormattable ? ((IFormattable)obj).ToString(null, CultureInfo.InvariantCulture)
                : obj.ToString();
        }
    }
}
