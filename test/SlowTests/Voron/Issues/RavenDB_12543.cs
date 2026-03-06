using System;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_12543 : StorageTest
    {
        public RavenDB_12543(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Must_not_update_PTT_during_stage2_of_commit()
        {
            RequireFileBasedPager();

            var buffer = new byte[256];

            new Random().NextBytes(buffer);
            
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");

                for (int i = 0; i < 100; i++)
                {
                    tree.Add("items/" + i, buffer);
                }

                tx.LowLevelTransaction.ForTestingPurposesOnly().SimulateThrowingOnCommitStage2 = true;

                Assert.Throws<InvalidOperationException>(() => tx.Commit());
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.EnsureNoDuplicateTransactionId_Forced(tx.LowLevelTransaction.Id);

                var tree = tx.CreateTree("test");

                for (int i = 0; i < 100; i++)
                {
                    tree.Add("items/" + i, buffer);
                }

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("test");

                for (int i = 0; i < 100; i++)
                {
                    Assert.True(tree.TryRead("items/" + i, out var reader));
                    Assert.Equal(buffer, reader.ReadBytes(reader.Length).ToArray());
                }

                tx.Commit();
            }
        }
    }
}
