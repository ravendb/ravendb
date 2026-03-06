using System;
using System.Collections.Generic;
using FastTests.Voron;
using SlowTests.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Voron.CompactTrees
{
    public class FuzzyUpsertsTests(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed(500000, 4096)]
        [InlineDataWithRandomSeed(100000, 1024)]
        [InlineDataWithRandomSeed(10000, 64)]
        public void RandomUpsertsWithoutRemoves(int treeSize, int transactionSize = 10000, int randomSeed = 1337)
        {
            var currentState = new Dictionary<long, long>();
            var keys = new List<long>();
            var rnd = new Random(randomSeed);

            var transactions = treeSize / transactionSize;

            // Initializing the tree with random writes. Depending on the tree size, it may
            // cause the creation of new dictionaries and transitioning pages. 
            int i = 0;
            while ( i < transactions + 1 )
            {
                int itemsToInsert = (i < transactions) ? transactionSize : treeSize % transactionSize;
                using (var wtx = Env.WriteTransaction())
                {
                    var tree = wtx.CompactTreeFor( "test");
                    for (int j = 0; j < itemsToInsert; j++)
                    {
                        long item = Math.Abs((long)rnd.Next() + rnd.Next());

                        keys.Add(item);
                        currentState[item] = item;
                        tree.Add(item.ToString(), item);
                    }
                    wtx.Commit();
                }

                i++;
            }

            // Run 10 batches of random upserts. 
            for (int batches = 0; batches < 10; batches++)
            {
                using (var wtx = Env.WriteTransaction())
                {
                    var tree = wtx.CompactTreeFor( "test");
                    for (int j = 0; j < treeSize; j++)
                    {
                        long key = keys[rnd.Next(keys.Count)];
                        long value = Math.Abs((long)rnd.Next() + rnd.Next());

                        currentState[key] = value;
                        tree.Add(key.ToString(), value);
                    }
                    wtx.Commit();
                }

                using (var rtx = Env.ReadTransaction())
                {
                    var tree = rtx.CompactTreeFor( "test");

                    foreach (var key in keys)
                    {
                        // We need to ensure the element IS there, and that it also has the right value
                        Assert.True(tree.TryGetValue(key.ToString(), out var r));
                        Assert.Equal(currentState[key], r);
                    }
                }
            }
        }


        [RavenTheory(RavenTestCategory.Voron)]
        [InlineDataWithRandomSeed(500000, 4096)]
        [InlineDataWithRandomSeed(100000, 1024)]
        [InlineDataWithRandomSeed(10000, 64)]
        [InlineData(500000, 4096, 2002966381)]
        public void RandomUpsertsWithRemoves(int treeSize, int transactionSize = 10000, int randomSeed = 1337)
        {
            var currentState = new Dictionary<long, long>();
            var currentKeys = new List<long>();
            var rnd = new Random(randomSeed);

            var transactions = treeSize / transactionSize;

            // Initializing the tree with random writes. Depending on the tree size, it may
            // cause the creation of new dictionaries and transitioning pages. 
            int i = 0;
            while (i < transactions + 1)
            {
                int itemsToInsert = (i < transactions) ? transactionSize : treeSize % transactionSize;
                using (var wtx = Env.WriteTransaction())
                {
                    var tree = wtx.CompactTreeFor( "test");
                    for (int j = 0; j < itemsToInsert; j++)
                    {
                        long item = Math.Abs((long)rnd.Next() + rnd.Next());

                        currentKeys.Add(item);
                        currentState[item] = item;
                        tree.Add(item.ToString(), item);
                    }
                    wtx.Commit();
                }

                i++;
            }

            var removedKeys = new HashSet<long>();

            // Run 10 batches of random upserts. 
            for (int batches = 0; batches < 10; batches++)
            {
                using (var wtx = Env.WriteTransaction())
                {
                    var tree = wtx.CompactTreeFor( "test");
                    for (int j = 0; j < treeSize; j++)
                    {
                        long key = currentKeys[rnd.Next(currentKeys.Count)];

                        // We will remove 1% of the inserts if we haven't remove it already... 
                        if (rnd.Next(100) <= 1 && !removedKeys.Contains(key))
                        {
                            Assert.True(tree.TryRemove(key.ToString(), out long oldValue));

                            // Ensure that the last known value is a match
                            Assert.Equal(currentState[key], oldValue);
                            removedKeys.Add(key);
                        }
                        else if (!removedKeys.Contains(key))
                        {
                            // Do an upsert instead.
                            long value = Math.Abs((long)rnd.Next() + rnd.Next());

                            currentState[key] = value;
                            tree.Add(key.ToString(), value);
                        }
                    }
                    wtx.Commit();
                }

                using (var rtx = Env.ReadTransaction())
                {
                    var tree = rtx.CompactTreeFor( "test");

                    foreach (var key in currentKeys)
                    {
                        if (removedKeys.Contains(key))
                        {
                            Assert.False(tree.TryGetValue(key.ToString(), out var _));
                        }
                        else
                        {
                            // We need to ensure the element IS there, and that it also has the right value
                            Assert.True(tree.TryGetValue(key.ToString(), out var r));
                            Assert.Equal(currentState[key], r);
                        }
                    }
                }
            }
        }
    }
}
