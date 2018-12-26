using System;
using FastTests.Voron;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow.LowMemory;
using Sparrow.Threading;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class BloomFilterTests : StorageTest
    {
        [Fact]
        public void Basic()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, SharedMultipleUseFlag.None))
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");

                    var key1 = context.GetLazyString("orders/1");
                    var key2 = context.GetLazyString("orders/2");

                    using (var filter = new CollectionOfBloomFilters.BloomFilter64(0, tree, writable: true, allocator: context.Allocator))
                    {
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
                        
                        context.ReturnMemory(key1.AllocatedMemoryData);
                        context.ReturnMemory(key2.AllocatedMemoryData);
                    }

                    tx.Commit();
                }
            }
        }

        [Fact]
        public void CanPersist()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, SharedMultipleUseFlag.None))
            {
                var key1 = context.GetLazyString("orders/1");

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");

                    using (var filter = new CollectionOfBloomFilters.BloomFilter64(0, tree, writable: true, allocator: context.Allocator))
                    {
                        Assert.True(filter.Add(key1));
                        Assert.Equal(1, filter.Count);

                        filter.Flush();
                    }

                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");

                    using (var filter = new CollectionOfBloomFilters.BloomFilter64(0, tree, writable: false, allocator: context.Allocator))
                    {
                        Assert.False(filter.Add(key1));
                        Assert.Equal(1, filter.Count);
                    }
                }

                context.ReturnMemory(key1.AllocatedMemoryData);
            }
        }

        [Fact]
        public void CheckWritability()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, SharedMultipleUseFlag.None))
            {
                var key1 = context.GetLazyString("orders/1");
                var key2 = context.GetLazyString("orders/2");

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");

                    using (var filter = new CollectionOfBloomFilters.BloomFilter64(0, tree, writable: true, allocator: context.Allocator))
                    {
                        Assert.True(filter.Add(key1));
                        Assert.Equal(1, filter.Count);
                        Assert.True(filter.Writable);
                        Assert.False(filter.ReadOnly);

                        filter.Flush();
                    }

                    tx.Commit();
                }

                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("Filters");

                    using (var filter = new CollectionOfBloomFilters.BloomFilter64(0, tree, writable: false, allocator: context.Allocator))
                    {
                        Assert.False(filter.Writable);

                        Assert.False(filter.Add(key1));
                        Assert.Equal(1, filter.Count);
                        Assert.False(filter.Writable);

                        Assert.True(filter.Add(key2));
                        Assert.Equal(2, filter.Count);
                        Assert.True(filter.Writable);
                    }
                }

                context.ReturnMemory(key1.AllocatedMemoryData);
                context.ReturnMemory(key2.AllocatedMemoryData);
            }
        }

        [Fact]
        public void CheckReadonly()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, SharedMultipleUseFlag.None))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    var tree = context.Transaction.InnerTransaction.CreateTree("BloomFilters");
                    tree.Increment("Count64", 2);

                    using (var filter = new CollectionOfBloomFilters.BloomFilter64(0, tree, writable: true, allocator: context.Allocator))
                    {
                        Assert.False(filter.ReadOnly);
                        filter.MakeReadOnly();
                        Assert.True(filter.ReadOnly);
                    }

                    tx.Commit();
                }

                using (context.OpenWriteTransaction())
                {
                    using (var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        Assert.Equal(2, collection.Count);

                        Assert.True(collection[0].ReadOnly);
                        Assert.False(collection[1].ReadOnly);
                    }
                }
            }
        }

        [Fact]
        public void WillExpand()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, SharedMultipleUseFlag.None))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X86, context))
                    {
                        Assert.Equal(1, collection.Count);

                        for (var i = 0; i < CollectionOfBloomFilters.BloomFilter32.MaxCapacity * 1.2; i++)
                        {
                            var key = context.GetLazyString("orders/" + i);
                            collection.Add(key);
                        }

                        Assert.Equal(2, collection.Count);
                    }

                    tx.Commit();
                }

                using (context.OpenWriteTransaction())
                {
                    using (var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X86, context))
                    {
                        Assert.Equal(2, collection.Count);
                    }
                }
            }
        }

        [Fact]
        public void CannotMixFilters()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, SharedMultipleUseFlag.None))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var collection = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X86, context))
                    {
                        Assert.Equal(1, collection.Count);

                        collection.AddFilter(collection.CreateNewFilter(1, CollectionOfBloomFilters.Mode.X86)); // should not throw

                        Assert.Throws<InvalidOperationException>(() => collection.AddFilter(collection.CreateNewFilter(2, CollectionOfBloomFilters.Mode.X64)));
                    }
                }
            }
        }
    }
}
