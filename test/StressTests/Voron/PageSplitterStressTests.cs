using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using FastTests.Voron;
using Voron;
using Xunit;

namespace StressTests.Voron
{
    public class PageSplitterStressTests : StorageTest
    {
        [Fact]
        public void PageSplitterShouldCalculateSeparatorKeyCorrectly()
        {
            var ids = ReadIds("data.txt");
            var env = Env;
            {
                var rand = new Random();
                var testBuffer = new byte[79];
                rand.NextBytes(testBuffer);

                var trees = CreateTrees(env, 1, "tree");

                var addedIds = new List<string>();
                foreach (var id in ids) // 244276974/13/250/2092845878 -> 8887 iteration
                {

                    using (var tx = env.WriteTransaction())
                    {
                        foreach (var treeName in trees)
                        {
                            var tree = tx.CreateTree(treeName);

                            tree.Add(id, new MemoryStream(testBuffer));
                        }

                        tx.Commit();

                        addedIds.Add(id);
                    }
                }

                ValidateRecords(env, trees, ids);
            }
        }

        [Fact]
        public void PageSplitterShouldCalculateSeparatorKeyCorrectly2()
        {
            var ids = ReadIds("data2.txt");

            StorageEnvironmentOptions storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
            storageEnvironmentOptions.MaxScratchBufferSize *=2;
            using (var env = new StorageEnvironment(storageEnvironmentOptions))
            {
                var rand = new Random();
                var testBuffer = new byte[69];
                rand.NextBytes(testBuffer);

                var trees = CreateTrees(env, 1, "tree");

                foreach (var id in ids)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        foreach (var treeName in trees)
                        {
                            var tree = tx.CreateTree(treeName);
                            tree.Add(id, new MemoryStream(testBuffer));
                        }

                        tx.Commit();
                    }
                }

                ValidateRecords(env, trees, ids);
            }
        }

        private void ValidateRecords(StorageEnvironment env, IEnumerable<string> trees, IList<string> ids)
        {
            using (var snapshot = env.ReadTransaction())
            {
                foreach (var tree in trees)
                {
                    var readTree = snapshot.ReadTree(tree);
                    using (var iterator = readTree.Iterate(false))
                    {
                        Assert.True(iterator.Seek(Slices.BeforeAllKeys));

                        var keys = new HashSet<string>();

                        var count = 0;
                        do
                        {
                            keys.Add(iterator.CurrentKey.ToString());
                            Assert.True(ids.Contains(iterator.CurrentKey.ToString()));
                            var readResult = readTree.Read( iterator.CurrentKey);
                            if (readResult == null)
                            {

                            }
                            Assert.NotNull(readResult);

                            count++;
                        }
                        while (iterator.MoveNext());

                        Assert.Equal(ids.Count, readTree.State.NumberOfEntries);
                        Assert.Equal(ids.Count, count);
                        Assert.Equal(ids.Count, keys.Count);
                    }
                }
            }
        }

        private static IList<string> ReadIds(string fileName, int maxSize = int.MaxValue)
        {
            var assembly = typeof(PageSplitterStressTests).GetTypeInfo().Assembly;
            using (var fs = assembly.GetManifestResourceStream("StressTests.Voron.Data." + fileName))
            using (var reader = new StreamReader(fs))
            {
                string line;

                var results = new List<string>();

                while (!string.IsNullOrEmpty(line = reader.ReadLine()) && results.Count < maxSize)
                {
                    results.Add(line.Trim());
                }

                return results;
            }
        }
    }
}
