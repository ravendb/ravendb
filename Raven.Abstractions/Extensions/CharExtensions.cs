using System;
using System.Globalization;
namespace Raven.Abstractions.Extensions
{
    public static class CharExtensions
    {
        public static string CharToString(this char c)
        {
#if !DNXCORE50
            return c.ToString(CultureInfo.InvariantCulture);
#else
            return c.ToString();
#endif
        }

        public static string ToInvariantString(this object obj)
        {
            return obj is IConvertible ? ((IConvertible)obj).ToString(CultureInfo.InvariantCulture)
                : obj is IFormattable ? ((IFormattable)obj).ToString(null, CultureInfo.InvariantCulture)
                : obj.ToString();
        }
    }
}
