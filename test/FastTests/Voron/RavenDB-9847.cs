using System.Linq;
using Xunit;
using Tests.Infrastructure;

namespace FastTests.Voron
{
    public class RavenDB_9847 : StorageTest
    {
        public RavenDB_9847(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Can_get_updated_overflow_value_in_same_tx()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");
                // set to 1
                tree.Add("one", Enumerable.Range(0, 1024 * 5).Select(i => (byte)1).ToArray());
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");
                // set to 2
                tree.Add("one", Enumerable.Range(0, 1024 * 5).Select(i => (byte)2).ToArray());

                Assert.True(tree.TryRead("one", out var reader));
                var bytes = reader.ReadBytes(1024 * 5);
                for (int i = 0; i < 1024*5; i++)
                {
                    Assert.Equal(2, bytes[i]);
                }
            }
        }
    }
}
