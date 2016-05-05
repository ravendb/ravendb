using System.IO;
using Xunit;
using Voron;
using Voron.Data;
using Voron.Debugging;

namespace FastTests.Voron.Bugs
{
    public class PagesFilteredOutByJournalApplicator : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);
            options.ManualFlushing = true;
        }

        [Fact]
        public void CouldNotReadPagesThatWereFilteredOutByJournalApplicator_1()
        {
            var bytes = new byte[1000];

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree("foo");

                tree.Add("bars/1", new MemoryStream(bytes));

                txw.Commit();

                DebugStuff.RenderAndShow(tree);
            }

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("bar");

                txw.Commit();
            }

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("baz");

                txw.Commit();
            }

            using (var txr = Env.ReadTransaction())
            {
                using (var txw = Env.WriteTransaction())
                {
                    var tree = txw.CreateTree("foo");

                    tree.Add("bars/1", new MemoryStream());

                    txw.Commit();

                }

                Env.FlushLogToDataFile();

                Assert.NotNull(txr.CreateTree( "foo").Read("bars/1"));
            }
        } 

        [Fact]
        public void CouldNotReadPagesThatWereFilteredOutByJournalApplicator_2()
        {
            var bytes = new byte[1000];

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree( "foo");

                tree.Add("bars/1", new MemoryStream(bytes));
                tree.Add("bars/2", new MemoryStream(bytes));
                tree.Add("bars/3", new MemoryStream(bytes));
                tree.Add("bars/4", new MemoryStream(bytes));

                txw.Commit();

                DebugStuff.RenderAndShow(tree);
            }

            using (var txw = Env.WriteTransaction())
            {
                var tree = txw.CreateTree(  "foo");

                tree.Add("bars/0", new MemoryStream());
                tree.Add("bars/5", new MemoryStream());

                txw.Commit();

                DebugStuff.RenderAndShow(tree);
            }

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree(  "bar");

                txw.Commit();
            }

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("baz");

                txw.Commit();
            }

            using (var txr = Env.ReadTransaction())
            {
                using (var txw = Env.WriteTransaction())
                {
                    var tree = txw.CreateTree(  "foo");

                    tree.Add("bars/4", new MemoryStream());

                    txw.Commit();

                }

                Env.FlushLogToDataFile();

                Assert.NotNull(txr.CreateTree( "foo").Read("bars/5"));
            }
        }
    }
}
