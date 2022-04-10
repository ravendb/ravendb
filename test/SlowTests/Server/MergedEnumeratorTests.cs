using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.Senders;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server
{
    public class MergedEnumeratorTests : NoDisposalNeeded
    {
        public MergedEnumeratorTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Returns_items_in_order()
        {
            using (var merged = new MergedEnumerator<int>(new IntComparer()))
            {
                var items1 = new List<int> { 2, 4, 6 };
                var items2 = new List<int> { 1, 3, 5 };

                merged.AddEnumerator(items1.GetEnumerator());
                merged.AddEnumerator(items2.GetEnumerator());

                for (var i = 1; i <= 6; i++)
                {
                    Assert.True(merged.MoveNext());
                    Assert.Equal(i, merged.Current);
                }
            }
        }

        [Fact]
        public async Task Returns_items_in_order_async()
        {
            await using (var merged = new MergedAsyncEnumerator<int>(new IntComparer()))
            {
                var items1 = new AsyncIntList(new List<int> { 2, 4, 6 });
                var items2 = new AsyncIntList(new List<int> { 1, 3, 5 });

                await merged.AddAsyncEnumerator(items1);
                await merged.AddAsyncEnumerator(items2);

                for (var i = 1; i <= 6; i++)
                {
                    Assert.True(await merged.MoveNextAsync());
                    Assert.Equal(i, merged.Current);
                }
            }
        }

        [Fact]
        public void Returns_items_in_order_for_ETL()
        {
            using (var merged = new ExtractedItemsEnumerator<Item, EtlStatsScope, EtlPerformanceOperation>(new EtlStatsScope(new EtlRunStats())))
            {
                var items1 = new List<Item>
                {
                    new Item(1), new Item(3), new Item(4)
                };

                var items2 = new List<Item>
                {
                    new Item(2), new Item(5), new Item(6)
                };

                merged.AddEnumerator(items1.GetEnumerator());
                merged.AddEnumerator(items2.GetEnumerator());

                for (var i = 1; i <= 6; i++)
                {
                    Assert.True(merged.MoveNext());
                    Assert.Equal(i, merged.Current.Etag);
                }
            }
        }

        private class IntComparer : IComparer<int>
        {
            public int Compare(int x, int y)
            {
                return x == y ? 0 : x < y ? -1 : 1;
            }
        }

        public class AsyncIntList : IAsyncEnumerator<int>
        {
            private readonly List<int> _list;
            private int _position;
            public AsyncIntList(List<int> list)
            {
                _list = list;
            }


            private int _current;

            public int Current => _current;

            public ValueTask<bool> MoveNextAsync()
            {
                if (_list.Count <= _position)
                    return ValueTask.FromResult(false);

                _current = _list[_position];
                _position++;
                return ValueTask.FromResult(true);
            }

            public ValueTask DisposeAsync()
            {
                return ValueTask.CompletedTask;
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
