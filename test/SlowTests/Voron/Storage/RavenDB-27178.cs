using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class RavenDB_27178 : FastTests.Voron.StorageTest
    {
        public RavenDB_27178(ITestOutputHelper output) : base(output)
        {
        }

        private sealed class State
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void AsyncCommitTransactionInheritsClientStateFromCommittingTransaction()
        {
            // DocumentsStorage stores its per-commit cache in the transaction's client state during
            // LastChanceToReadFromWriteTransactionBeforeCommit (i.e. in CommitStage1). when a write commit
            // starts the next transaction asynchronously, that transaction must inherit the state the
            // committing transaction just computed - which is ahead of what the environment has published,
            // since the environment only publishes when the async commit completes.
            var state = new State();

            var tx1 = Env.WriteTransaction();
            try
            {
                // nothing has set it yet on a freshly opened write transaction
                Assert.False(tx1.LowLevelTransaction.TryGetClientState<State>(out _));

                // dirty the transaction so the async commit really goes through the journal path
                var tree = tx1.CreateTree("foo");
                tree.Add("a", StreamFor("1"));

                // runs in CommitStage1, triggered by BeginAsyncCommitAndStartNewTransaction below
                tx1.LowLevelTransaction.LastChanceToReadFromWriteTransactionBeforeCommit +=
                    llt => llt.UpdateClientState(state);

                using (var tx2 = tx1.BeginAsyncCommitAndStartNewTransaction(tx1.LowLevelTransaction.PersistentContext))
                {
                    // tx2 was cloned right after CommitStage1 ran the handler above, so it carries the freshly
                    // computed state - not missing, and not a stale value published by the environment
                    Assert.True(tx2.LowLevelTransaction.TryGetClientState<State>(out var inherited));
                    Assert.Same(state, inherited);

                    using (tx1)
                    {
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
        }
    }
}
