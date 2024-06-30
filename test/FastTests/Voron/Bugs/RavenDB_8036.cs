using System.Linq;
using Tests.Infrastructure;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_8036 : StorageTest
    {
        public RavenDB_8036(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }


        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.AllX64)]
        public void ModifySamePageAfterItWasFlushed()
        {
            Options.ManualFlushing = true;
            var tx1 = Env.WriteTransaction();

            try
            {
                var allocatePage1 = tx1.LowLevelTransaction.AllocatePage(1).PageNumber;
                var allocatePage2 = tx1.LowLevelTransaction.AllocatePage(2).PageNumber;

                using (var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction(tx1.LowLevelTransaction.PersistentContext))
                {
                    var modifiedPage1 = tx2.LowLevelTransaction.ModifyPage(allocatePage1);

                    var txId  = tx1.LowLevelTransaction.Id;
                    var what = tx1.LowLevelTransaction.CurrentStateRecord.ScratchPagesTable
                        .Any(x => x.Value.AllocatedInTransaction > txId);
                    Assert.False(what);

                    using (tx1)
                    {
                        tx1.EndAsyncCommit();
                    }
                    what = Env.CurrentStateRecord.ScratchPagesTable
                        .Any(x => x.Value.AllocatedInTransaction > txId);
                    Assert.False(what);
                    var modifiedPage2 = tx2.LowLevelTransaction.ModifyPage(allocatePage2);
                    what = Env.CurrentStateRecord.ScratchPagesTable
                        .Any(x => x.Value.AllocatedInTransaction > txId);
                    Assert.False(what);

                    tx1 = null;

                    tx2.Commit();
                }
            }
            finally
            {
                tx1?.Dispose();
            }
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.AllX64)]
        public void Flushing_should_not_throw_on_freeing_scratch_page_async_commit()
        {
            var tx1 = Env.WriteTransaction();

            try
            {
                var allocatePage = tx1.LowLevelTransaction.AllocatePage(1);

                using (var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction(tx1.LowLevelTransaction.PersistentContext))
                {
                    using (tx1)
                    {
                        tx2.LowLevelTransaction.FreePage(allocatePage.PageNumber); // free the same page that was allocated by the parent tx

                        tx1.EndAsyncCommit();
                    }

                    tx1 = null;

                    tx2.Commit();
                }
            }
            finally
            {
                tx1?.Dispose();
            }

            using (var tx = Env.WriteTransaction())
            {
                // just to increment transaction id

                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }

            Env.FlushLogToDataFile();
        }

        [Fact]
        public void Flushing_should_not_throw_on_freeing_scratch_page()
        {
            long pageNumber;

            using (var tx1 = Env.WriteTransaction())
            {
                pageNumber = tx1.LowLevelTransaction.AllocatePage(1).PageNumber;
                tx1.Commit();
            }

            using (var tx2 = Env.WriteTransaction())
            {
                tx2.LowLevelTransaction.FreePage(pageNumber); // free the same page that was allocated by the prev tx

                tx2.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                // just to increment transaction id

                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }

            Env.FlushLogToDataFile();
        }
    }
}
