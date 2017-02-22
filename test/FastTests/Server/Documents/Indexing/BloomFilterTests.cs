using System;
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
                    byte* ptr;
                    tree.DirectAdd(key, CollectionOfBloomFilters.BloomFilter64.PtrSize, out ptr);

                    var key1 = context.GetLazyString("orders/1");
                    var key2 = context.GetLazyString("orders/2");

                    var filter = new CollectionOfBloomFilters.BloomFilter64(key, ptr, tree, writeable: true);

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

                    context.ReturnMemory(key1.AllocatedMemoryData);
                    context.ReturnMemory(key2.AllocatedMemoryData);
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
                    byte* ptr;
                    tree.DirectAdd(key, CollectionOfBloomFilters.BloomFilter64.PtrSize,out ptr);

                    var filter = new CollectionOfBloomFilters.BloomFilter64(key, ptr, tree, writeable: true);

                    Assert.True(filter.Add(key1));
                    Assert.Equal(1, filter.Count);

                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");
                    var read = tree.Read("f1");

                    var filter = new CollectionOfBloomFilters.BloomFilter64(key, read.Reader.Base, tree, writeable: false);

                    Assert.False(filter.Add(key1));
                    Assert.Equal(1, filter.Count);
                }

                context.ReturnMemory(key1.AllocatedMemoryData);
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
                    byte* ptr;
                    tree.DirectAdd(key, CollectionOfBloomFilters.BloomFilter64.PtrSize,out ptr);

                    var filter = new CollectionOfBloomFilters.BloomFilter64(key, ptr, tree, writeable: true);

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

                    var filter = new CollectionOfBloomFilters.BloomFilter64(key, read.Reader.Base, tree, writeable: false);
                    Assert.False(filter.Writeable);

                    Assert.False(filter.Add(key1));
                    Assert.Equal(1, filter.Count);
                    Assert.False(filter.Writeable);

                    Assert.True(filter.Add(key2));
                    Assert.Equal(2, filter.Count);
                    Assert.True(filter.Writeable);
                }

                context.ReturnMemory(key1.AllocatedMemoryData);
                context.ReturnMemory(key2.AllocatedMemoryData);
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
                    byte* ptr;
                    tree.DirectAdd(key1, CollectionOfBloomFilters.BloomFilter64.PtrSize,out ptr).Dispose(); // filter 1
                    byte* _;
                    tree.DirectAdd(key2, CollectionOfBloomFilters.BloomFilter64.PtrSize,out _).Dispose(); // filter 2

                    var filter = new CollectionOfBloomFilters.BloomFilter64(key1, ptr, tree, writeable: true);
                    Assert.False(filter.ReadOnly);
                    filter.MakeReadOnly();
                    Assert.True(filter.ReadOnly);

                    tx.Commit();
                }

                using (context.OpenWriteTransaction())
                {
                    var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context);
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
                using (var tx = context.OpenWriteTransaction())
                {
                    var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X86, context);
                    Assert.Equal(1, collection.Count);

                    for (var i = 0; i < CollectionOfBloomFilters.BloomFilter32.MaxCapacity * 1.2; i++)
                    {
                        var key = context.GetLazyString("orders/" + i);
                        collection.Add(key);
                    }

                    Assert.Equal(2, collection.Count);

                    tx.Commit();
                }

                using (context.OpenWriteTransaction())
                {
                    var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X86, context);
                    Assert.Equal(2, collection.Count);
                }
            }
        }

        [Fact]
        public void CannotMixFilters()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X86, context);
                    Assert.Equal(1, collection.Count);

                    collection.AddFilter(collection.CreateNewFilter(1, CollectionOfBloomFilters.Mode.X86)); // should not throw

                    Assert.Throws<InvalidOperationException>(() => collection.AddFilter(collection.CreateNewFilter(2, CollectionOfBloomFilters.Mode.X64)));
                }
            }
        }
    }
}