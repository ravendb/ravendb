using System;
using FastTests.Voron;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron
{
    public class RavenDB_16428 : StorageTest
    {
        public RavenDB_16428(ITestOutputHelper output) : base(output)
        {
        }

        private const int _64KB = 64 * 1024;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = _64KB * 4;
            options.ManualFlushing = true;
            options.ManualSyncing = true;
        }

        [Fact]
        public void MustNotThrowObjectDisposedExceptionWhenFreeingPagesOnTxRollback()
        {
            RequireFileBasedPager();

            try
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("tree");

                    var r = new Random(1);

                    for (int i = 0; i < 8; i++)
                    {
                        var overflowSize = r.Next(300, 1000);

                        var bytes = new byte[overflowSize * 8192];

                        tree.Add("items/" + i, bytes);
                    }

                    throw new InvalidOperationException("Force transaction rollback");
                }
            }
            catch (Exception)
            {
                // expected
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");

                var r = new Random(1);

                var overflowSize = r.Next(300, 1000);

                var bytes = new byte[overflowSize * 8192];

                tree.Add("items/1", bytes);

                // tx.Commit(); - intentionally not committing
            }
        }
    }
}
