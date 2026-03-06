using System;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RDBCL_1977 : StorageTest
    {
        public RDBCL_1977(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void MustNotAllowToCreateWriteTransactionAfterCatastrophicError()
        {
            var exceptionDuringStage3OfCommit = Assert.Throws<InvalidOperationException>(() =>
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.LowLevelTransaction.AfterCommitWhenNewTransactionsPrevented += transaction => throw new InvalidOperationException(
                        "Intentional error during CommitStage3_DisposeTransactionResources should mark the env in the catastrophic error state");

                    tx.Commit();
                }
            });

            // this should throw because of AssertNoCatastrophicFailure() assertion on write tx creation
            var exceptionOnTxCreation = Assert.Throws<InvalidOperationException>(() =>
            {
                using (Env.WriteTransaction())
                {

                }
            });

            Assert.Equal(exceptionDuringStage3OfCommit.Message, exceptionOnTxCreation.Message);

            Assert.Throws<InvalidOperationException>(() => Env.Options.AssertNoCatastrophicFailure());

            // Read transaction can be still created 
            using (Env.ReadTransaction())
            {

            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void MustNotAllowToCommitWriteTransactionDuringAfterCommitWhenNewTransactionsPrevented()
        {
            var ex = Assert.Throws<InvalidOperationException>(() =>
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.LowLevelTransaction.AfterCommitWhenNewTransactionsPrevented += transaction =>
                    {
                        transaction.Commit();
                    };

                    tx.Commit();
                }
            });

            Assert.Equal("Cannot commit already committed transaction.", ex.Message);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void MustNotAllowToCreateWriteTransactionDuringAfterCommitWhenNewTransactionsPrevented()
        {
            var ex = Assert.Throws<InvalidOperationException>(() =>
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.LowLevelTransaction.AfterCommitWhenNewTransactionsPrevented += transaction =>
                    {
                        using (Env.WriteTransaction())
                        {

                        }
                    };

                    tx.Commit();
                }
            });

            Assert.Contains("A write transaction is already opened", ex.Message);
        }
    }
}
