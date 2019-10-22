using System;
using System.IO;
using Voron;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class PageTableIssue : FastTests.Voron.StorageTest
    {
        public PageTableIssue(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);
            options.ManualFlushing = true;
        }

        [Fact]
        public void MissingScratchPagesInPageTable()
        {
            var bytes = new byte[1000];

            using (var txw = Env.WriteTransaction())
            {
                var tree1 = txw.CreateTree("foo");
                var tree2 = txw.CreateTree("bar");
                var tree3 = txw.CreateTree("baz");

                tree1.Add("foos/1", new MemoryStream(bytes));

                txw.Commit();

            }

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree( "bar");

                tree.Add("bars/1", new MemoryStream(bytes));

                txw.Commit();
            }

            var bytesToFillFirstJournalCompletely = new byte[8*Constants.Storage.PageSize];

            new Random().NextBytes(bytesToFillFirstJournalCompletely);

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree("baz");

                // here we have to put a big value to be sure that in next transaction we will put the
                // updated value into a new journal file - this is the key to expose the issue
                tree.Add("bazs/1", new MemoryStream(bytesToFillFirstJournalCompletely));

                txw.Commit();

            }

            using (var txr = Env.ReadTransaction())
            {
                using (var txw = Env.WriteTransaction())
                {
                    var tree = txw.CreateTree(  "foo");

                    tree.Add("foos/1", new MemoryStream());

                    txw.Commit();

                }

                Env.FlushLogToDataFile();

                Assert.NotNull(txr.CreateTree("foo").Read("foos/1"));
            }
        }
    }
}
