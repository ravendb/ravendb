using FastTests.Voron;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Voron;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public unsafe class BloomFilterTests : StorageTest
    {
        [Fact]
        public void Basic()
        {
            Slice key;
            using (var context = new TransactionOperationContext(Env, 1024, 1024))
            using (Slice.From(context.Allocator, "f1", out key))
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var ptr = tree.DirectAdd(key, CollectionOfBloomFilters.BloomFilter.PtrSize);

                    var key1 = context.GetLazyString("orders/1");
                    var key2 = context.GetLazyString("orders/2");

                    var filter = new CollectionOfBloomFilters.BloomFilter(key, ptr, tree, writeable: true);

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
            Slice key;
            using (var context = new TransactionOperationContext(Env, 1024, 1024))
            using (Slice.From(context.Allocator, "f1", out key))
            {
                var key1 = context.GetLazyString("orders/1");

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var ptr = tree.DirectAdd(key, CollectionOfBloomFilters.BloomFilter.PtrSize);

                    var filter = new CollectionOfBloomFilters.BloomFilter(key, ptr, tree, writeable: true);

                    Assert.True(filter.Add(key1));
                    Assert.Equal(1, filter.Count);

                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var read = tree.Read("f1");

                    var filter = new CollectionOfBloomFilters.BloomFilter(key, read.Reader.Base, tree, writeable: false);

                    Assert.False(filter.Add(key1));
                    Assert.Equal(1, filter.Count);
                }
            }
        }

        [Fact]
        public void CheckWriteability()
        {
            Slice key;
            using (var context = new TransactionOperationContext(Env, 1024, 1024))
            using (Slice.From(context.Allocator, "f1", out key))
            {
                var key1 = context.GetLazyString("orders/1");
                var key2 = context.GetLazyString("orders/2");

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var ptr = tree.DirectAdd(key, CollectionOfBloomFilters.BloomFilter.PtrSize);

                    var filter = new CollectionOfBloomFilters.BloomFilter(key, ptr, tree, writeable: true);

                    Assert.True(filter.Add(key1));
                    Assert.Equal(1, filter.Count);
                    Assert.True(filter.Writeable);
                    Assert.False(filter.ReadOnly);

                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var read = tree.Read("f1");

                    var filter = new CollectionOfBloomFilters.BloomFilter(key, read.Reader.Base, tree, writeable: false);
                    Assert.False(filter.Writeable);

                    Assert.False(filter.Add(key1));
                    Assert.Equal(1, filter.Count);
                    Assert.False(filter.Writeable);

                    Assert.True(filter.Add(key2));
                    Assert.Equal(2, filter.Count);
                    Assert.True(filter.Writeable);
                }
            }
        }

        [Fact]
        public void CheckReadonly()
        {
            Slice key1, key2;
            using (var context = new TransactionOperationContext(Env, 1024, 1024))
            using (Slice.From(context.Allocator, "0000", out key1))
            using (Slice.From(context.Allocator, "0001", out key2))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    var tree = context.Transaction.InnerTransaction.CreateTree("IndexedDocs");
                    var ptr = tree.DirectAdd(key1, CollectionOfBloomFilters.BloomFilter.PtrSize); // filter 1
                    tree.DirectAdd(key2, CollectionOfBloomFilters.BloomFilter.PtrSize); // filter 2

                    var filter = new CollectionOfBloomFilters.BloomFilter(key1, ptr, tree, writeable: true);
                    Assert.False(filter.ReadOnly);
                    filter.MakeReadOnly();
                    Assert.True(filter.ReadOnly);

                    tx.Commit();
                }

                using (context.OpenWriteTransaction())
                {
                    var collection = CollectionOfBloomFilters.Load(1024, context);
                    Assert.Equal(2, collection.Count);

                    Assert.True(collection[0].ReadOnly);
                    Assert.False(collection[1].ReadOnly);
                }
            }
        }

        [Fact]
        public void WillExpand()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024))
            {
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