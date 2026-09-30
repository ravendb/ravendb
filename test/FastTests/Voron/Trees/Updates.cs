using System;
using System.IO;
using Tests.Infrastructure;
using Voron.Global;
using Xunit;

namespace FastTests.Voron.Trees
{
    public class Updates(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void CanUpdateVeryLargeValueAndThenDeleteIt()
        {
            var random = new Random();
            var buffer = new byte[Constants.Storage.PageSize*2];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("a", new MemoryStream(buffer));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Assert.Equal(4, tree.State.Header.PageCount);
                Assert.Equal(3, tree.State.Header.OverflowPages);
            }

            buffer = new byte[Constants.Storage.PageSize * 2 * 2];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                tree.Add("a", new MemoryStream(buffer));

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Assert.Equal(6, tree.State.Header.PageCount);
                Assert.Equal(5, tree.State.Header.OverflowPages);
            }
        }


        [RavenFact(RavenTestCategory.Voron)]
        public void CanAddAndUpdate()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                tree.Add("test", StreamFor("1"));
                tree.Add("test", StreamFor("2"));

                var readKey = ReadKey(tx, tree, "test");
                Assert.Equal("test", readKey.Item1.ToString());
                Assert.Equal("2", readKey.Item2.ToString());
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanAddAndUpdate2()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                tree.Add("test/1", StreamFor("1"));
                tree.Add("test/2", StreamFor("2"));
                tree.Add("test/1", StreamFor("3"));

                var readKey = ReadKey(tx, tree, "test/1");
                Assert.Equal("test/1", readKey.Item1.ToString());
                Assert.Equal("3", readKey.Item2.ToString());

                readKey = ReadKey(tx, tree, "test/2");
                Assert.Equal("test/2", readKey.Item1.ToString());
                Assert.Equal("2", readKey.Item2.ToString());

            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanAddAndUpdate1()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                tree.Add("test/1", StreamFor("1"));
                tree.Add("test/2", StreamFor("2"));
                tree.Add("test/2", StreamFor("3"));

                var readKey = ReadKey(tx, tree, "test/1");
                Assert.Equal("test/1", readKey.Item1.ToString());
                Assert.Equal("1", readKey.Item2.ToString());

                readKey = ReadKey(tx, tree, "test/2");
                Assert.Equal("test/2", readKey.Item1.ToString());
                Assert.Equal("3", readKey.Item2.ToString());

            }
        }


        [RavenFact(RavenTestCategory.Voron)]
        public void CanDelete()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                tree.Add("test", StreamFor("1"));
                Assert.NotNull(ReadKey(tx, tree, "test"));

                tree.Delete("test");
                Assert.Null(ReadKey(tx, tree, "test"));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanDelete2()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                tree.Add("test/1", StreamFor("1"));
                tree.Add("test/2", StreamFor("1"));
                Assert.NotNull(ReadKey(tx, tree, "test/2"));

                tree.Delete("test/2");
                Assert.Null(ReadKey(tx, tree, "test/2"));
                Assert.NotNull(ReadKey(tx, tree, "test/1"));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanDelete1()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                tree.Add("test/1", StreamFor("1"));
                tree.Add("test/2", StreamFor("1"));
                Assert.NotNull(ReadKey(tx, tree, "test/1"));

                tree.Delete("test/1");
                Assert.Null(ReadKey(tx, tree, "test/1"));
                Assert.NotNull(ReadKey(tx, tree, "test/2"));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void OverwriteThatShrinksOverflowValueReadsBackInSameTransaction()
        {
            var value1 = new byte[33_000];
            var value2 = new byte[20_000];
            new Random(1).NextBytes(value1);
            new Random(2).NextBytes(value2);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("t");
                tree.Add("key", value1);
                tree.Add("key", value2);

                Assert.True(tree.TryRead("key", out var read));
                Assert.Equal(value2.Length, read.Length);

                var actual = new byte[value2.Length];
                read.Read(actual, 0, actual.Length);
                Assert.Equal(value2, actual);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ThirdOverwriteDoesNotFreeNeighboursPages()
        {
            var v1 = new byte[33_000];
            var v2 = new byte[20_000];
            var v3 = new byte[10_000];
            var n = new byte[10_000];
            new Random(1).NextBytes(v1);
            new Random(2).NextBytes(v2);
            new Random(3).NextBytes(v3);
            new Random(4).NextBytes(n);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("t");
                tree.Add("K", v1);
                tree.Add("K", v2); // shrink frees pages the next key can claim
                tree.Add("N", n);
                tree.Add("K", v3); // a stale overflow header here would free N's pages

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("t");

                Assert.True(tree.TryRead("N", out var readN));
                Assert.Equal(n.Length, readN.Length);
                var actualN = new byte[n.Length];
                readN.Read(actualN, 0, actualN.Length);
                Assert.Equal(n, actualN);

                Assert.True(tree.TryRead("K", out var readK));
                Assert.Equal(v3.Length, readK.Length);
                var actualK = new byte[v3.Length];
                readK.Read(actualK, 0, actualK.Length);
                Assert.Equal(v3, actualK);
            }
        }
    }
}
