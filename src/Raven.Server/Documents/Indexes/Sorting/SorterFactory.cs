using System;
using System.Collections.Generic;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Indexes.Sorting
{
    public class SorterFactory
    {
        public readonly Type Type;

        public SorterFactory(Type sorterType)
        {
            Type = sorterType ?? throw new ArgumentNullException(nameof(sorterType));
        }

        public virtual FieldComparator CreateInstance(string fieldName, int numHits, int sortPos, bool reversed, List<string> diagnostics)
        {
            var instance = Activator.CreateInstance(Type, fieldName, numHits, sortPos, reversed, diagnostics) as FieldComparator;
            if (instance == null)
                throw new InvalidOperationException($"Created sorter does not inherit from '{nameof(FieldComparator)}' class.");

            return instance;
        }
    }
}
