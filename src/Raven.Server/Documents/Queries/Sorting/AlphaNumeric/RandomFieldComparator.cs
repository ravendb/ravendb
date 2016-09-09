using System;
using Lucene.Net.Index;
using Lucene.Net.Search;

namespace Raven.Server.Documents.Queries.Sorting.AlphaNumeric
{
    public class RandomFieldComparator : FieldComparator
    {
        private readonly Random _random;
        private readonly int[] _values;
        private int _bottom; // Value of bottom of queue
        private int[] _currentReaderValues;

        internal RandomFieldComparator(string field, int numHits)
        {
            _values = new int[numHits];
            _random = new Random(field.GetHashCode());
        }

        public override int Compare(int slot1, int slot2)
        {
            var v1 = _values[slot1];
            var v2 = _values[slot2];
            if (v1 > v2)
                return 1;
            if (v1 < v2)
                return -1;
            return 0;
        }

        public override int CompareBottom(int doc)
        {
            var v2 = _currentReaderValues[doc];
            if (_bottom > v2)
                return 1;
            if (_bottom < v2)
                return -1;
            return 0;
        }

        public override void Copy(int slot, int doc)
        {
            _values[slot] = _currentReaderValues[doc];
        }

        public override void SetNextReader(IndexReader reader, int docBase)
        {
            _currentReaderValues = new int[reader.MaxDoc];
            for (int i = 0; i < _currentReaderValues.Length; i++)
            {
                _currentReaderValues[i] = _random.Next();
            }
        }

        public override IComparable this[int slot] => _values[slot];

        public override void SetBottom(int bottom)
        {
            _bottom = _values[bottom];
        }
    }
}
