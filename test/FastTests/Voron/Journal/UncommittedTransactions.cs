using Tests.Infrastructure;
using Xunit;
using Voron;
using Voron.Global;

namespace FastTests.Voron.Journal
{
    public class UncommittedTransactions : StorageTest
    {
        public UncommittedTransactions(ITestOutputHelper output) : base(output)
        {
        }

        // all tests here relay on the fact than one log file can contains max 10 pages
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 10 * Constants.Storage.PageSize;
        }


        [RavenFact(RavenTestCategory.Voron)]
        public void UncommittedTransactionMustNotModifyPageTranslationTableOfLogFile()
        {
            long pageAllocatedInUncommittedTransaction;
            using (var tx1 = Env.WriteTransaction())
            {
                var page = tx1.LowLevelTransaction.AllocatePage(1);

                pageAllocatedInUncommittedTransaction = page.PageNumber;

                Assert.NotNull(tx1.LowLevelTransaction.GetPage(pageAllocatedInUncommittedTransaction));
                
                // tx.Commit(); do not commit
            }
            Assert.False(Env.WriteTransactionPool.ScratchPagesInUse.ContainsKey(pageAllocatedInUncommittedTransaction));
        }
    }
}
