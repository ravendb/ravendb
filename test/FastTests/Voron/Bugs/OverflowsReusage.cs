using System;
using Voron;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public class OverflowsReusage : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Theory]
        [InlineData(2500, 2400)]
        [InlineData(7000, 5000)]
        [InlineData(9000, 2000)]
        public void FaultyOptimization_ReadTransactionCannotSeeUncommittedValue(int overflowSize1, int overflowSize2)
        {
            var r = new Random();
            var bytes1 = new byte[overflowSize1];
            r.NextBytes(bytes1);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");

                tree.Add("key", bytes1);

                tx.Commit();
            }

            Env.FlushLogToDataFile();

            var bytes2 = new byte[overflowSize2];
            r.NextBytes(bytes2);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                tree.Add("key", bytes2); // TryOverwriteOverflowPages will be used under the hood 

                using (var readTransaction = Env.ReadTransaction())
                {
                    var readTree = readTransaction.ReadTree("tree");

                    var read = new byte[overflowSize1];
                    readTree.Read("key").Reader.Read(read, 0, overflowSize1);

                    Assert.Equal(bytes1, read); // bytes2 isn't committed yet
                }

                tx.Commit();
            }
        }

        [Theory]
        [InlineData(2500, 2400)]
        [InlineData(7000, 5000)]
        [InlineData(9000, 2000)]
        public void FaultyOptimization_OverflowPageIsntFlushedToJournal(int overflowSize1, int overflowSize2)
        {
            RequireFileBasedPager();

            var r = new Random();
            var bytes1 = new byte[overflowSize1];
            r.NextBytes(bytes1);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");

                tree.Add("key", bytes1);

                tx.Commit();
            }

            var bytes2 = new byte[overflowSize2];
            r.NextBytes(bytes2);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("tree");

                tree.Add("key", bytes2); // TryOverwriteOverflowPages will be used under the hood 

                tx.Commit();
            }

            //RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("tree");
                
                var valueReader = tree.Read("key").Reader;
                var read = new byte[valueReader.Length];
                valueReader.Read(read, 0, read.Length);

                Assert.Equal(bytes2, read);
            }
        }
    }
}