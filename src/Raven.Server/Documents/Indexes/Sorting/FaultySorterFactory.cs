using System;
using System.Collections.Generic;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Indexes.Sorting
{
    public class FaultySorterFactory : SorterFactory
    {
        private readonly string _name;
        private readonly Exception _e;

        public FaultySorterFactory(string name, Exception e)
            : base(typeof(FieldComparator))
        {
            _name = name;
            _e = e;
        }

        public override FieldComparator CreateInstance(string fieldName, int numHits, int sortPos, bool reversed, List<string> diagnostics)
        {
            throw new NotSupportedException($"Sorter {_name} is an implementation of a faulty sorter", _e);
        }
    }
}
