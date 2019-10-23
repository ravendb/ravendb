// -----------------------------------------------------------------------
//  <copyright file="DataCorruptionInOverflow.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class DataCorruptionInOverflow : FastTests.Voron.StorageTest
    {
        public DataCorruptionInOverflow(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_RavenDB_2585()
        {
            const int testedOverflowSize = 20000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(  "test");

                var itemBytes = new byte[16000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);

                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tree.Delete("items/1");
                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("test");

                var readResult = tree.Read("items/3");

                var readBytes = new byte[testedOverflowSize];

                readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                Assert.Equal(overflowValue, readBytes);
            }
        }

        [Fact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_2_RavenDB_2585()
        {
            const int testedOverflowSize = 16000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(  "test");

                var itemBytes = new byte[2000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);


                itemBytes = new byte[30000];
                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree( "test");
                tree.Delete("items/1");
                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("test");

                var readResult = tree.Read("items/3");

                var readBytes = new byte[testedOverflowSize];

                readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                Assert.Equal(overflowValue, readBytes);
            }
        }

        [Fact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_RavenDB_2806()
        {
            RequireFileBasedPager();

            const int testedOverflowSize = 20000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);


            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(  "test");

                var itemBytes = new byte[16000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);

                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tree.Delete("items/1");
                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("test");

                var readResult = tree.Read("items/3");

                var readBytes = new byte[testedOverflowSize];

                readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                Assert.Equal(overflowValue, readBytes);
            }
        }

        [Fact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_2_RavenDB_2806()
        {
            RequireFileBasedPager();

            const int testedOverflowSize = 16000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);


            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(  "test");

                var itemBytes = new byte[2000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);


                itemBytes = new byte[30000];
                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tx.Commit();
            }


            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(  "test");
                tree.Delete("items/1");
                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("test");

                var readResult = tree.Read("items/3");

                var readBytes = new byte[testedOverflowSize];

                readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                Assert.Equal(overflowValue, readBytes);
            }
        }

        [Fact]
        public void IncorrectWriteOfOverflowPagesFromJournalsToDataFile_RavenDB_2891()
        {
            const int testedOverflowSize = 50000;

            var overflowValue = new byte[testedOverflowSize];
            new Random(1).NextBytes(overflowValue);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree(  "test");

                var itemBytes = new byte[30000];

                new Random(2).NextBytes(itemBytes);
                tree.Add("items/1", itemBytes);

                new Random(3).NextBytes(itemBytes);
                tree.Add("items/2", itemBytes);

                tree.Delete("items/1");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("test");

                tree.Delete("items/2");

                tree.Add("items/3", overflowValue);

                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("test");

                var readResult = tree.Read("items/3");

                var readBytes = new byte[testedOverflowSize];

                readResult.Reader.Read(readBytes, 0, testedOverflowSize);

                Assert.Equal(overflowValue, readBytes);
            }
        }
    }
}
