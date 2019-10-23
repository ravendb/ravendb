using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public unsafe class UpdateLastItem : FastTests.Voron.StorageTest
    {
        public UpdateLastItem(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            byte* ptr;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("t");
                tree.DirectAdd("events", sizeof(TreeRootHeader), out ptr).Dispose();
                tree.DirectAdd("aggregations", sizeof(TreeRootHeader), out ptr).Dispose();
                tree.DirectAdd("aggregation-status", sizeof(TreeRootHeader), out ptr).Dispose();
                tx.Commit();
            }
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("t");
                tree.DirectAdd("events", sizeof(TreeRootHeader), out ptr).Dispose();

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("t");
                tree.DirectAdd("events", sizeof(TreeRootHeader), out ptr).Dispose();

                tx.Commit();
            }
        }
    }
}
