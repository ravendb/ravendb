// -----------------------------------------------------------------------
//  <copyright file="UncommittedTransactions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Xunit;
using Voron;
using Voron.Global;
using Xunit.Abstractions;

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


        [Fact]
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
            using (var tx2 = Env.ReadTransaction())
            {
                // tx was not committed so in the log should not apply
                var readPage = Env.Journal.ReadPage(tx2.LowLevelTransaction,pageAllocatedInUncommittedTransaction);

                Assert.Null(readPage);
            }
        }
    }
}
