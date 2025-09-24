using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Server.Documents.Queries.Sorting.AlphaNumeric;

namespace SlowTests.Data.RavenDB_8355
{
    public class MySorterWithDiagnostics : FieldComparator
    {
        private readonly List<string> _diagnostics;
        private readonly AlphaNumericFieldComparator _inner;

        public MySorterWithDiagnostics(string fieldName, int numHits, int sortPos, bool reversed, List<string> diagnostics)
        {
            _diagnostics = diagnostics;
            _inner = new AlphaNumericFieldComparator(fieldName, numHits);
        }

        public override int Compare(int slot1, int slot2)
        {
            _diagnostics.Add("Inner");
            return _inner.Compare(slot1, slot2);
        }

        public override void SetBottom(int slot)
        {
            _diagnostics.Add("Inner");
            _inner.SetBottom(slot);
        }

        public override int CompareBottom(int doc, IState state)
        {
            _diagnostics.Add("Inner");
            return _inner.CompareBottom(doc, state);
        }

        public override void Copy(int slot, int doc, IState state)
        {
            _diagnostics.Add("Inner");
            _inner.Copy(slot, doc, state);
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            _diagnostics.Add("Inner");
            _inner.SetNextReader(reader, docBase, state);
        }

        public override IComparable this[int slot]
        {
            get
            {
                _diagnostics.Add("Inner");
                return _inner[slot];
            }
        }
    }
}
