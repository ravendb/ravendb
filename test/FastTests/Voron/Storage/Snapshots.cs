using System.IO;
using System.Text;
using Xunit;
using Voron;

namespace FastTests.Voron.Storage
{
    public class Snapshots : StorageTest
    {


        [Fact]
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
                var reader = tree.Read("key/1").Reader;
                Assert.Equal("123", reader.ToStringValue());
                tx.Commit();
            }
        }
    }
}