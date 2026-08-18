using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.Journal
{
    public class TotalCommittedSinceLastFlushPagesTests(ITestOutputHelper output) : StorageTest(output)
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CommittedPagesAreCountedForRegularTransactions()
        {
            const int numberOfTransactions = 20;
            var value = new byte[512];

            long previous = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

            for (int i = 0; i < numberOfTransactions; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("key/" + i, value);
                    tx.Commit();
                }

                long current = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

                Assert.True(current > previous,
                    $"After commit #{i + 1} the flusher should have been told about the pages that transaction wrote, " +
                    $"but TotalCommittedSinceLastFlushPages stayed at {current} (was {previous} before the commit).");

                previous = current;
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CommittedPagesAreCountedForAsyncCommittedTransactions()
        {
            const int numberOfTransactions = 20;
            var value = new byte[512];

            long previous = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

            var tx = Env.WriteTransaction();
            try
            {
                for (int i = 0; i < numberOfTransactions; i++)
                {
                    tx.CreateTree("tree").Add("key/" + i, value);

                    var next = tx.BeginAsyncCommitAndStartNewTransaction(tx.LowLevelTransaction.PersistentContext);

                    using (tx)
                    {
                        tx.EndAsyncCommit();
                    }

                    tx = next;

                    long current = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

                    Assert.True(current > previous,
                        $"After async commit #{i + 1} the flusher should have been told about the pages that transaction wrote, " +
                        $"but TotalCommittedSinceLastFlushPages stayed at {current} (was {previous} before the commit). " +
                        "When async-committed pages are not counted, the size-based flush trigger never fires and " +
                        "flushing falls back to the timer, which lets the scratch buffers grow without bound.");

                    previous = current;
                }

                tx.Commit();
            }
            finally
            {
                tx?.Dispose();
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CounterGrowsByTheNumberOfPagesTheTransactionActuallyDirtied()
        {
            var value = new byte[8192];

            for (int i = 0; i < 20; i++)
            {
                long before = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;
                long modified;

                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("key/" + i, value);
                    modified = tx.LowLevelTransaction.NumberOfModifiedPages;
                    tx.Commit();
                }

                long delta = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages - before;

                Assert.True(delta >= modified,
                    $"Transaction #{i + 1} dirtied {modified} pages but only {delta} were added to " +
                    $"TotalCommittedSinceLastFlushPages. Under-counting here delays the size-based flush trigger, " +
                    "which is the only trigger that fires in practice.");
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CounterReachesTheFlushThresholdUnderSustainedWrites()
        {
            long threshold = Env.Options.MaxNumberOfPagesInJournalBeforeFlush;
            var value = new byte[8192];

            const int maxTransactions = 5000;
            int committed = 0;

            while (committed < maxTransactions && Env.Journal.Applicator.TotalCommittedSinceLastFlushPages < threshold)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("key/" + committed, value);
                    tx.Commit();
                }

                committed++;
            }

            long reached = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

            Assert.True(reached >= threshold,
                $"After {committed} committed transactions TotalCommittedSinceLastFlushPages only reached {reached}, " +
                $"below the {threshold} pages that trigger a flush. The size-based flush trigger cannot fire, " +
                "so flushing is left entirely to the timer.");
        }
    }
}
