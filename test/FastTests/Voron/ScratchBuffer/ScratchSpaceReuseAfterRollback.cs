using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.ScratchBuffer
{
    public class ScratchSpaceReuseAfterRollback : StorageTest
    {
        public ScratchSpaceReuseAfterRollback(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            base.Configure(options);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Pages_freed_by_rolled_back_transactions_are_reused_and_scratch_space_stays_constant()
        {
            var overflowValue = new string('a', 20000); // stored as an overflow, allocated as a 4 pages block in the scratch space

            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("foo").Add("blocked", overflowValue);
                txw.Commit();
            }

            using (var txw = Env.WriteTransaction())
            {
                // supersedes the pages written by the first transaction
                txw.CreateTree("foo").Add("blocked", new string('b', 20000));
                txw.Commit();
            }

            using (Env.ReadTransaction())
            {
                // frees the superseded pages - but with 'valid after the flush txid' markers, so as long as
                // the read transaction above stays open, those free pages are not allowed to be reused
                Env.FlushLogToDataFile();

                var scratchFile = Env.ScratchBufferPool._current.File;

                // warm-up: the first rolled back transaction allocates the working set once
                RollbackTransactionAddingTempValue(overflowValue);

                var before = scratchFile.LastUsedPage;

                const int rolledBackTransactions = 50;

                for (var i = 0; i < rolledBackTransactions + 1; i++)
                {
                    // no reader could ever see the pages of a rolled back transaction,
                    // so every next transaction is expected to reuse them immediately
                    RollbackTransactionAddingTempValue(overflowValue);
                }

                var after = scratchFile.LastUsedPage;

                Assert.True(after == before,
                    $"the scratch space grew from {before} to {after} pages ({(after - before) / rolledBackTransactions} pages per transaction) " +
                    $"over {rolledBackTransactions} rolled back transactions, although each of them freed everything it allocated. ");
            }
        }

        private void RollbackTransactionAddingTempValue(string value)
        {
            using (var txw = Env.WriteTransaction())
            {
                txw.CreateTree("foo").Add("temp", value);

                // no Commit() - the pages this transaction allocated were never visible to anyone
            }
        }
    }
}
