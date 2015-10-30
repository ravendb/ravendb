using System;
using System.IO;
using Xunit;

namespace Voron.Tests.Trees
{
    public class Updates : StorageTest
    {
        [PrefixesFact]
        public void CanUpdateVeryLargeValueAndThenDeleteIt()
        {
            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);

            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("a", new MemoryStream(buffer));

                tx.Commit();
            }

            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Assert.Equal(4, tx.Root.State.PageCount);
                Assert.Equal(3, tx.Root.State.OverflowPages);
            }

            buffer = new byte[8192 * 2];
            random.NextBytes(buffer);


            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("a", new MemoryStream(buffer));

                tx.Commit();
            }


            using (var tx = Env.NewTransaction(TransactionFlags.Read))
            {
                Assert.Equal(6, tx.Root.State.PageCount);
                Assert.Equal(5, tx.Root.State.OverflowPages);				
            }
        }


        [PrefixesFact]
        public void CanAddAndUpdate()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("test", StreamFor("1"));
                tx.Root.Add			("test", StreamFor("2"));

                var readKey = ReadKey(tx, "test");
                Assert.Equal("test", readKey.Item1);
                Assert.Equal("2", readKey.Item2);
            }
        }

        [PrefixesFact]
        public void CanAddAndUpdate2()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("test/1", StreamFor("1"));
                tx.Root.Add			("test/2", StreamFor("2"));
                tx.Root.Add			("test/1", StreamFor("3"));

                var readKey = ReadKey(tx, "test/1");
                Assert.Equal("test/1", readKey.Item1);
                Assert.Equal("3", readKey.Item2);

                readKey = ReadKey(tx, "test/2");
                Assert.Equal("test/2", readKey.Item1);
                Assert.Equal("2", readKey.Item2);

            }
        }

        [PrefixesFact]
        public void CanAddAndUpdate1()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("test/1", StreamFor("1"));
                tx.Root.Add			("test/2", StreamFor("2"));
                tx.Root.Add			("test/2", StreamFor("3"));

                var readKey = ReadKey(tx, "test/1");
                Assert.Equal("test/1", readKey.Item1);
                Assert.Equal("1", readKey.Item2);

                readKey = ReadKey(tx, "test/2");
                Assert.Equal("test/2", readKey.Item1);
                Assert.Equal("3", readKey.Item2);

            }
        }


        [PrefixesFact]
        public void CanDelete()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("test", StreamFor("1"));
                Assert.NotNull(ReadKey(tx, "test"));

                tx.Root.Delete("test");
                Assert.Null(ReadKey(tx, "test"));
            }
        }

        [PrefixesFact]
        public void CanDelete2()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("test/1", StreamFor("1"));
                tx.Root.Add			("test/2", StreamFor("1"));
                Assert.NotNull(ReadKey(tx, "test/2"));

                tx.Root.Delete("test/2");
                Assert.Null(ReadKey(tx, "test/2"));
                Assert.NotNull(ReadKey(tx, "test/1"));
            }
        }

        [PrefixesFact]
        public void CanDelete1()
        {
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                tx.Root.Add			("test/1", StreamFor("1"));
                tx.Root.Add			("test/2", StreamFor("1"));
                Assert.NotNull(ReadKey(tx, "test/1"));

                tx.Root.Delete("test/1");
                Assert.Null(ReadKey(tx, "test/1"));
                Assert.NotNull(ReadKey(tx, "test/2"));
            }
        }
    }
}
