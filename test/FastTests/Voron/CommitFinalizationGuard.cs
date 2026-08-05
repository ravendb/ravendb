using System;
using System.Text;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public class CommitFinalizationGuard : StorageTest
    {
        public CommitFinalizationGuard(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SubscriberFailureAfterJournalWriteTakesEnvironmentDown()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.LowLevelTransaction.BeforeCommitFinalization += _ => throw new InvalidOperationException("subscriber failed");

                Assert.Throws<InvalidOperationException>(() => tx.Commit());
            }

            Assert.True(Env.Options.IsCatastrophicFailureSet);

            // The journal already holds the transaction. Serving further writes as if it rolled
            // back would diverge from what recovery will replay. The background flusher may have
            // wrapped the stored failure by now. Assert on the exception chain, not the type.
            var e = Assert.ThrowsAny<Exception>(() => Env.WriteTransaction().Dispose());
            Assert.Contains("subscriber failed", FlattenMessages(e));
        }

        [RavenFact(RavenTestCategory.Voron)]
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

        [RavenFact(RavenTestCategory.Voron)]
        public void AfterCommitSubscriberFailureUnderPreventedTransactionsTakesEnvironmentDown()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.LowLevelTransaction.AfterCommitWhenNewTransactionsPrevented += _ => throw new InvalidOperationException("subscriber failed");

                Assert.Throws<InvalidOperationException>(() => tx.Commit());
            }

            Assert.True(Env.Options.IsCatastrophicFailureSet);
        }

        private static string FlattenMessages(Exception e)
        {
            var sb = new StringBuilder();
            for (; e != null; e = e.InnerException)
                sb.AppendLine(e.Message);
            return sb.ToString();
        }
    }
}
