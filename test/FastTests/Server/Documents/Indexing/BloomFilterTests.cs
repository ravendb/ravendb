using FastTests.Voron;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public unsafe class BloomFilterTests : StorageTest
    {
        [Fact]
        public void Basic()
        {
            using (var context = new JsonOperationContext(1024, 1024))
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("Filters");
                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var ptr = tree.DirectAdd("f1", CollectionOfBloomFilters.BloomFilter.PtrSize);

                    var key1 = context.GetLazyString("orders/1");
                    var key2 = context.GetLazyString("orders/2");

                    var filter = new CollectionOfBloomFilters.BloomFilter(0, ptr);

                    Assert.False(filter.Contains(key1));
                    Assert.False(filter.Contains(key2));
                    Assert.Equal(0, filter.Count);

                    Assert.True(filter.Add(key1));
                    Assert.True(filter.Contains(key1));
                    Assert.False(filter.Contains(key2));
                    Assert.Equal(1, filter.Count);

                    Assert.False(filter.Add(key1));
                    Assert.True(filter.Contains(key1));
                    Assert.False(filter.Contains(key2));
                    Assert.Equal(1, filter.Count);

                    Assert.True(filter.Add(key2));
                    Assert.True(filter.Contains(key1));
                    Assert.True(filter.Contains(key2));
                    Assert.Equal(2, filter.Count);

                    Assert.False(filter.Add(key1));
                    Assert.True(filter.Contains(key1));
                    Assert.True(filter.Contains(key2));
                    Assert.Equal(2, filter.Count);

                    tx.Commit();
                }
            }
        }

        [Fact]
        public void CanPersist()
        {
            using (var context = new JsonOperationContext(1024, 1024))
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("Filters");
                    tx.Commit();
                }

                var key1 = context.GetLazyString("orders/1");

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var ptr = tree.DirectAdd("f1", CollectionOfBloomFilters.BloomFilter.PtrSize);

                    var filter = new CollectionOfBloomFilters.BloomFilter(0, ptr);

                    Assert.True(filter.Add(key1));
                    Assert.Equal(1, filter.Count);

                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var read = tree.Read("f1");

                    var filter = new CollectionOfBloomFilters.BloomFilter(0, read.Reader.Base);

                    Assert.False(filter.Add(key1));
                    Assert.Equal(1, filter.Count);
                }
            }
        }

        [Fact]
        public void WillExpand()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024))
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("Filters");
                    tx.Commit();
                }

                var key1 = context.GetLazyString("orders/1");
                var key2 = context.GetLazyString("orders/2");

                using (var tx = context.OpenWriteTransaction())
                {
                    var collection = CollectionOfBloomFilters.Load(2, context);
                    Assert.Equal(1, collection.Count);

                    Assert.True(collection.Add(key1));
                    Assert.Equal(1, collection.Count);

                    Assert.True(collection.Add(key2));
                    Assert.Equal(2, collection.Count);

                    tx.Commit();
                }

                using (context.OpenWriteTransaction())
                {
                    var collection = CollectionOfBloomFilters.Load(2, context);
                    Assert.Equal(2, collection.Count);
                }
            }
        }
    }
}