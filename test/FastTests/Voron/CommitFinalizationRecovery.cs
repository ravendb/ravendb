using System;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron
{
    public class CommitFinalizationRecovery : StorageTest
    {
        public CommitFinalizationRecovery(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void FailedCommitAfterJournalWriteDivergesFromRecovery()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("key", "value");
                tx.LowLevelTransaction.BeforeCommitFinalization += _ => throw new InvalidOperationException("subscriber failed");

                Assert.Throws<InvalidOperationException>(() => tx.Commit());
            }

            bool catastrophic = Env.Options.IsCatastrophicFailureSet;

            bool visibleBeforeRestart;
            using (Env.Options.SkipCatastrophicFailureAssertion())
            using (var tx = Env.ReadTransaction())
                visibleBeforeRestart = tx.ReadTree("tree")?.Read("key") != null;

            using (Env.Options.SkipCatastrophicFailureAssertion())
                RestartDatabase();

            bool visibleAfterRestart;
            using (var tx = Env.ReadTransaction())
                visibleAfterRestart = tx.ReadTree("tree")?.Read("key") != null;

            // the journal holds the write the caller observed as rolled back, so recovery replays it
            Assert.False(visibleBeforeRestart);
            Assert.True(visibleAfterRestart);

            // in-memory state and the journaled state disagree: the environment must not keep serving
            Assert.True(catastrophic);
        }
    }
}
