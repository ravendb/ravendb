using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Raven.Client.Util
{
    internal sealed class NewLineInsensitiveStringComparer : IEqualityComparer<string>
    {
        private static readonly Regex _regex = new Regex("\r*\n", RegexOptions.Compiled);

        public static NewLineInsensitiveStringComparer Instance = new NewLineInsensitiveStringComparer();

        private NewLineInsensitiveStringComparer()
        {
        }

        public bool Equals(string x, string y)
        {
            if (x == y)
                return true;
            if (x == null || y == null)
                return false;

            var xCoverted = Convert(x);
            var yConverted = Convert(y);

            return xCoverted == yConverted;
        }

        public int GetHashCode(string obj)
        {
            return Convert(obj)?.GetHashCode() ?? 0;
        }

        private static string Convert(string toConvert)
        {
            if (string.IsNullOrEmpty(toConvert))
                return toConvert;

            return _regex.Replace(toConvert, Environment.NewLine);
        }
    }
}
