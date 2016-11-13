using System;
using System.Collections.Generic;
using Raven.Abstractions.Indexing;

namespace Raven.NewClient.Client.Document
{
    //from issue http://issues.hibernatingrhinos.com/issue/RavenDB-3543
    internal class SortOptionsEqualityProvider : IEqualityComparer<KeyValuePair<string, SortOptions?>>
    {
        public static SortOptionsEqualityProvider Instance = new SortOptionsEqualityProvider();

        public bool Equals(KeyValuePair<string, SortOptions?> x, KeyValuePair<string, SortOptions?> y)
        {
            return x.Key.Equals(y.Key, StringComparison.OrdinalIgnoreCase);
        }

        public int GetHashCode(KeyValuePair<string, SortOptions?> obj)
        {
            return obj.Key.GetHashCode();
        }
    }
}
