using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using Voron;
using Xunit;

namespace SlowTests.Voron
{
    public class PageSplitter : StorageTest
    {
        readonly Random _random = new Random(1234);

        private string RandomString(int size)
        {
            var builder = new StringBuilder();
            for (int i = 0; i < size; i++)
            {
                builder.Append(Convert.ToChar(Convert.ToInt32(Math.Floor(26 * _random.NextDouble() + 65))));
            }

            return builder.ToString();
        }

        //Voron must support this in order to support MultiAdd() with values > 2000 characters
        [Fact]
        public void TreeAdds_WithVeryLargeKey()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree( "foo");
                tx.Commit();
            }

            var inputData = new List<string>();
            for (int i = 0; i < 1000; i++)
            {
                inputData.Add(RandomString(1024));
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int index = 0; index < inputData.Count; index++)
                {
                    var keyString = inputData[index];
                    tree.Add(keyString, new MemoryStream(new byte[] { 1, 2, 3, 4 }));
                }

                tx.Commit();
            }
        }

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
        public void PageSplitter_SmallRun()
        {
            var ids = ReadIds("data.txt", 35);
            var env = Env;
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

                        var readResult = tree.Read(id);

                        Assert.NotNull(readResult);
                    }

                    tx.Commit();

                    addedIds.Add(id);
                }
            }

            ValidateRecords(env, trees, ids);
        }

        [Fact]
        public void PageSplitterShouldCalculateSeparatorKeyCorrectly2()
        {
            var ids = ReadIds("data2.txt");

            StorageEnvironmentOptions storageEnvironmentOptions = StorageEnvironmentOptions.CreateMemoryOnly();
            storageEnvironmentOptions.MaxScratchBufferSize *=2;
            using (var env = new StorageEnvironment(storageEnvironmentOptions, NullLoggerSetup))
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
            var assembly = typeof(PageSplitter).GetTypeInfo().Assembly;
            using (var fs = assembly.GetManifestResourceStream("SlowTests.Data." + fileName))
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

        [Fact]
        public void ShouldNotThrowPageFullExceptionDuringPageSplit()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree( "foo");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");

                var normal = new byte[150];

                var small = new byte[0];

                var large = new byte[366];

                new Random(1).NextBytes(small);

                tree.Add("01", small);
                tree.Add("02", small);
                tree.Add("03", small);
                tree.Add("04", small);
                tree.Add("05", small);
                tree.Add("06", small);
                tree.Add("07", small);
                tree.Add("08", small);
                tree.Add("09", small);
                tree.Add("10", small);
                tree.Add("11", large);
                tree.Add("12", large);
                tree.Add("13", large);
                tree.Add("14", large);
                tree.Add("15", large);
                tree.Add("16", large);
                tree.Add("17", large);
                tree.Add("18", large);
                tree.Add("19", large);
                tree.Add("21", large);
                tree.Add("22", large);
                tree.Add("23", large);
                tree.Add("24", normal);
                tree.Add("25", normal);
                tree.Add("26", normal);
                tree.Add("27", normal);
                tree.Add("28", normal);
                tree.Add("29", normal);
                tree.Add("30", normal);


                const int toInsert = 230;

                tree.Add("20", new byte[toInsert]);

            }
        }

        [Fact]
        public void ShouldNotThrowPageFullExceptionDuringPageSplit2()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree( "foo");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");

                tree.Add("thumbproducts/57337", new byte[1998]);
                tree.Add("thumbproducts/57338", new byte[1993]);

                tree.Add("thumbproducts/573370", new byte[2016]); // originally here the exception was thrown during a page split
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("foo");

                Assert.Equal(1998, tree.Read("thumbproducts/57337").Reader.Length);
                Assert.Equal(1993, tree.Read("thumbproducts/57338").Reader.Length);
                Assert.Equal(2016, tree.Read("thumbproducts/573370").Reader.Length);
            }
        }
    }
}