using System.Collections;
using System.Collections.Generic;
using JetBrains.Annotations;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Static
{
    public partial class DynamicArray
    {
        [CanBeNull]
        private static NonGenericComparerAdapter AsObjectComparer([CanBeNull] IComparer comparer)
        {
            if (comparer == null)
                return null;

            return new NonGenericComparerAdapter(comparer);
        }

        private static object ConvertComparerValue(object value)
        {
            return value is LazyStringValue or LazyCompressedStringValue
                ? value.ToString()
                : value;
        }

        private sealed class NonGenericComparerAdapter : IComparer<object>
        {
            private readonly IComparer _comparer;

            public NonGenericComparerAdapter(IComparer comparer)
            {
                _comparer = comparer;
            }

            public int Compare(object x, object y)
            {
                return _comparer.Compare(ConvertComparerValue(x), ConvertComparerValue(y));
            }
        }
    }
}
