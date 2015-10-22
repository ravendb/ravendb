using Voron.Trees;

namespace Voron.Tests.Bugs
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;

    using Xunit;

    public class LargeValues : StorageTest
    {
        [Fact]
        public void ShouldProperlyRecover()
        {
            var sequentialLargeIds = ReadData("non-leaf-page-seq-id-large-values-2.txt");

            var enumerator = sequentialLargeIds.GetEnumerator();

            if (Directory.Exists("tests"))
                Directory.Delete("tests", true);

            var options = StorageEnvironmentOptions.ForPath("tests");
            options.ManualFlushing = true;

            using (var env = new StorageEnvironment(options))
            {
                for (var transactions = 0; transactions < 100; transactions++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("foo");
                        for (var i = 0; i < 100; i++)
                        {
                            enumerator.MoveNext();

                            tree.Add			(enumerator.Current.Key.ToString("0000000000000000"), new MemoryStream(enumerator.Current.Value));
                        }

                        tx.Commit();
                    }

                    if (transactions == 50)
                        env.FlushLogToDataFile();
                }

                ValidateRecords(env, new List<string> { "foo" }, sequentialLargeIds.Select(x => x.Key.ToString("0000000000000000")).ToList());
            }

            options = StorageEnvironmentOptions.ForPath("tests");
            options.ManualFlushing = true;

            using (var env = new StorageEnvironment(options))
            {
                ValidateRecords(env, new List<string> { "foo" }, sequentialLargeIds.Select(x => x.Key.ToString("0000000000000000")).ToList());
            }
        }

        private void ValidateRecords(StorageEnvironment env, IEnumerable<string> trees, IList<string> ids)
        {
            using (var snapshot = env.ReadTransaction())
            {
                foreach (var treeName in trees)
                {
                    var tree = snapshot.ReadTree(treeName);
                    using (var iterator = tree.Iterate())
                    {
                        Assert.True(iterator.Seek(Slice.BeforeAllKeys));

                        var keys = new HashSet<string>();

                        var count = 0;
                        do
                        {
                            keys.Add(iterator.CurrentKey.ToString());
                            Assert.True(ids.Contains(iterator.CurrentKey.ToString()), "Unknown key: " + iterator.CurrentKey);
                            Assert.NotNull(tree.Read(iterator.CurrentKey));

                            count++;
                        }
                        while (iterator.MoveNext());

                        Assert.Equal(ids.Count, tree.State.NumberOfEntries);
                        Assert.Equal(ids.Count, count);
                        Assert.Equal(ids.Count, keys.Count);
                    }
                }
            }
        }

        private IDictionary<long, byte[]> ReadData(string fileName)
        {
            using (var reader = new StreamReader("Bugs/Data/" + fileName))
            {
                string line;

                var random = new Random();
                var results = new Dictionary<long, byte[]>();

                while (!string.IsNullOrEmpty(line = reader.ReadLine()))
                {
                    var l = line.Trim().Split(':');

                    var buffer = new byte[int.Parse(l[1])];
                    random.NextBytes(buffer);

                    results.Add(long.Parse(l[0]), buffer);
                }

                return results;
            }
        }
    }
}
