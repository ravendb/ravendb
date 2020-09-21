using System;
using System.Collections;
using System.Collections.Generic;
using FastTests;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class MergedEnumeratorTests : NoDisposalNeeded
    {
        public MergedEnumeratorTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Returns_items_in_order()
        {
            using (var merged = new ExtractedItemsEnumerator<Item>(new EtlStatsScope(new EtlRunStats())))
            {
                var items1 = new List<Item>
                {
                    new Item(1), new Item(3), new Item(4)
                };

                var items2 = new List<Item>
                {
                    new Item(2), new Item(5), new Item(6)
                };

                merged.AddEnumerator(new MyExtractor(items1));
                merged.AddEnumerator(new MyExtractor(items2));

                for (var i = 1; i <= 6; i++)
                {
                    Assert.True(merged.MoveNext());
                    Assert.Equal(i, merged.Current.Etag);
                }
            }
        }

        private class MyExtractor : IExtractEnumerator<Item>
        {
            private readonly List<Item> _items;
            private int _index = 0;
            public MyExtractor(List<Item> items)
            {
                _items = items;
            }

            public bool Filter() => false;

            public bool MoveNext()
            {
                if (_index >= _items.Count)
                    return false;

                Current = _items[_index];
                _index++;
                return true;
            }

            public void Reset()
            {
                throw new System.NotImplementedException();
            }

            public Item Current { get; private set; }

            object? IEnumerator.Current => Current;

            public void Dispose()
            {
                
            }
        }

        private class Item : ExtractedItem
        {
            public Item(long etag)
            {
                Etag = etag;
                Type = EtlItemType.Document;
            }
        }
    }
}
