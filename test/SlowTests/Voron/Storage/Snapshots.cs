using System.IO;
using System.Text;
using Xunit;
using Tests.Infrastructure;

namespace SlowTests.Voron.Storage
{
    public class Snapshots : FastTests.Voron.StorageTest
    {
        public Snapshots(ITestOutputHelper output) : base(output)
        {
        }



        [RavenFact(RavenTestCategory.Voron)]
        public void SingleItemBatchTestLowLevel()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");

                tree.Add("key/1", new MemoryStream(Encoding.UTF8.GetBytes("123")));

                tx.Commit();
            }


            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Assert.True(tree.TryRead("key/1", out var reader));
                Assert.Equal("123", reader.ToStringValue());
                tx.Commit();
            }
        }
    }
}
