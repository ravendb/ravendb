using System;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

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

        [Fact]
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
                    var valueReader = tree.Read("items/" + i).Reader;
                    Assert.Equal(buffer, valueReader.ReadBytes(valueReader.Length).ToArray());
                }

                tx.Commit();
            }
        }
    }
}
