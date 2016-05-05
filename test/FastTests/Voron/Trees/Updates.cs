using System;
using System.IO;
using Xunit;
using Voron;
using Voron.Impl;

namespace FastTests.Voron.Trees
{
    public class Updates : StorageTest
    {

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.PageSize = 4 * Constants.Size.Kilobyte;

            base.Configure(options);
        }

        [Fact]
        public void CanUpdateVeryLargeValueAndThenDeleteIt()
        {
            var random = new Random();
            var buffer = new byte[8192];
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
                Assert.Equal(4, tree.State.PageCount);
                Assert.Equal(3, tree.State.OverflowPages);
            }

            buffer = new byte[8192 * 2];
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
                Assert.Equal(6, tree.State.PageCount);
                Assert.Equal(5, tree.State.OverflowPages);
            }
        }


        [Fact]
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

        [Fact]
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

        [Fact]
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


        [Fact]
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

        [Fact]
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

        [Fact]
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

     
    }
}
