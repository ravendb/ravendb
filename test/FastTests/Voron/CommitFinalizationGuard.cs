using System;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron
{
    public class CommitFinalizationGuard : StorageTest
    {
        public CommitFinalizationGuard(ITestOutputHelper output) : base(output)
        {
        }

        // A background flush can piggyback its journal flush-state update onto any running write
        // transaction and then set its own catastrophic failure, which overwrites the one under test.
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SubscriberFailureAfterJournalWriteTakesEnvironmentDown()
        {
            Exception subscriberFailure;
            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.LowLevelTransaction.BeforeCommitFinalization += _ => throw new InvalidOperationException("subscriber failed");

                subscriberFailure = Assert.Throws<InvalidOperationException>(() => tx.Commit());
            }

            Assert.Equal("subscriber failed", subscriberFailure.Message);

            // The journal already holds the transaction. Serving further writes as if it rolled
            // back would diverge from what recovery will replay.
            Assert.True(Env.Options.IsCatastrophicFailureSet);
            Assert.ThrowsAny<Exception>(() => Env.WriteTransaction().Dispose());
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.X64)]
        public void SubscriberFailureAfterAsyncJournalWriteTakesEnvironmentDown()
        {
            var tx = Env.WriteTransaction();
            try
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.LowLevelTransaction.BeforeCommitFinalization += _ => throw new InvalidOperationException("subscriber failed");

                var next = tx.BeginAsyncCommitAndStartNewTransaction(tx.LowLevelTransaction.PersistentContext);
                try
                {
                    Assert.Throws<InvalidOperationException>(() => tx.EndAsyncCommit());
                }
                finally
                {
                    next.Dispose();
                }
            }
            finally
            {
                tx.Dispose();
            }

            Assert.True(Env.Options.IsCatastrophicFailureSet);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SubscriberFailureWithNothingJournaledIsNotCatastrophic()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.BeforeCommitFinalization += _ => throw new InvalidOperationException("subscriber failed");

                Assert.Throws<InvalidOperationException>(() => tx.Commit());
            }

            Assert.False(Env.Options.IsCatastrophicFailureSet);

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void LastChanceSubscriberFailureBeforeJournalWriteIsNotCatastrophic()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.LowLevelTransaction.LastChanceToReadFromWriteTransactionBeforeCommit += _ => throw new InvalidOperationException("subscriber failed");

                Assert.Throws<InvalidOperationException>(() => tx.Commit());
            }

            Assert.False(Env.Options.IsCatastrophicFailureSet);

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }
        }
    }
}
