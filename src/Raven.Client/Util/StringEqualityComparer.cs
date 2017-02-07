using System;
using System.Collections.Generic;

namespace Raven.Server.Documents.Indexes.MapReduce.Static
{
    public class StringEqualityComparer : IEqualityComparer<string>
    {
        private readonly StringComparison _comparisonType;

        public StringEqualityComparer(StringComparison comparisonType = StringComparison.OrdinalIgnoreCase)
        {
            _comparisonType = comparisonType;
        }
        public bool Equals(string x, string y)
        {
            return x.Equals(y, _comparisonType);
        }

        public int GetHashCode(string obj)
        {
            return obj.GetHashCode();
        }
    }
}