using System;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;

namespace SlowTests.Data.RavenDB_8355
{
    public class MySorter : FieldComparator
    {
        private readonly string _args;

        public MySorter(string fieldName, int numHits, int sortPos, bool reversed, List<string> diagnostics)
        {
            _args = $"{fieldName}:{numHits}:{sortPos}:{reversed}";
        }

        public override int Compare(int slot1, int slot2)
        {
            throw new InvalidOperationException($"Catch me: {_args}");
        }

        public override void SetBottom(int slot)
        {
            throw new InvalidOperationException($"Catch me: {_args}");
        }

        public override int CompareBottom(int doc, IState state)
        {
            throw new InvalidOperationException($"Catch me: {_args}");
        }

        public override void Copy(int slot, int doc, IState state)
        {
            throw new InvalidOperationException($"Catch me: {_args}");
        }

        public override void SetNextReader(IndexReader reader, int docBase, IState state)
        {
            throw new InvalidOperationException($"Catch me: {_args}");
        }

        public override IComparable this[int slot] => throw new InvalidOperationException($"Catch me: {_args}");
    }
}
