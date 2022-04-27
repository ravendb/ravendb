using System;
using System.Collections.Generic;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.CompactTrees;
using Xunit;
using Xunit.Abstractions;
using static Voron.Data.CompactTrees.CompactTree;

namespace SlowTests.Voron
{

    public class CompactTreesTest : StorageTest
    {
        public CompactTreesTest(ITestOutputHelper output) : base(output)
        {
        }

        public enum SamplingMethod
        {
            FullRandom,
            BranchRandom,
            FullScan,
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1337, 200000, SamplingMethod.FullScan)]
        [InlineData(1337, 200000, SamplingMethod.FullRandom)]
        [InlineData(1337, 200000, SamplingMethod.BranchRandom)]
        public void CanRecompressItemsWithDeletesAndInserts(int seed, int size, SamplingMethod samplingMode)
        {
            static void Shuffle(string[] list, Random rng)
            {
                int n = list.Length;
                while (n > 1)
                {
                    n--;
                    int k = rng.Next(n + 1);
                    var value = list[k];
                    list[k] = list[n];
                    list[n] = value;
                }
            }

            Random random = new Random(seed);

            var uniqueKeys = new HashSet<string>();
            var inTreeKeys = new HashSet<string>();
            var removedKeys = new HashSet<string>();

            for (int iter = 0; iter < 4; iter++)
            {
                using (var wtx = Env.WriteTransaction())
                {
                    var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                    for (int i = 0; i < size; i++)
                    {
                        var rname = random.Next();
                        var key = "hi" + rname;
                        if (!uniqueKeys.Contains(key))
                        {
                            uniqueKeys.Add(key);
                            inTreeKeys.Add(key);
                            tree.Add(key, rname);
                        }
                    }

                    int samples = (int)tree.State.NumberOfEntries / 10;
                    if (samplingMode == SamplingMethod.FullRandom)
                        tree.TryImproveDictionary(
                            new RandomKeyScanner(tree, samples, seed),
                            new SequentialKeyScanner(tree));
                    else if (samplingMode == SamplingMethod.BranchRandom)
                        tree.TryImproveDictionary(
                            new RandomBranchKeyScanner(tree, samples, seed),
                            new SequentialKeyScanner(tree));
                    else
                        tree.TryImproveDictionary(
                            new SequentialKeyScanner(tree),
                            new SequentialKeyScanner(tree));

                    wtx.Commit();
                }

                var values = inTreeKeys.ToArray();
                Shuffle(values, random);

                using (var wtx = Env.WriteTransaction())
                {
                    var tree = CompactTree.Create(wtx.LowLevelTransaction, "test");
                    for (int i = 0; i < size / 2; i++)
                    {
                        Assert.True(tree.TryRemove(values[i], out var v));
                        inTreeKeys.Remove(values[i]);
                        removedKeys.Add(values[i]);
                    }
                    wtx.Commit();
                }
            }

            using (var rtx = Env.ReadTransaction())
            {
                var tree = CompactTree.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(inTreeKeys.Count, tree.State.NumberOfEntries);
                Assert.True(inTreeKeys.Count <= tree.State.NextTrainAt);

                foreach (var key in inTreeKeys)
                    Assert.True(tree.TryGetValue(key, out var v));

                foreach (var key in removedKeys)
                    Assert.False(tree.TryGetValue(key, out var v));
            }
        }
    }
}
