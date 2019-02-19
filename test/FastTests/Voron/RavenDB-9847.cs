using System.Linq;
using Xunit;

namespace FastTests.Voron
{
    public class RavenDB_9847 : StorageTest
    {
        [Fact]
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

                var reader = tree.Read("one");
                Assert.NotNull(reader);
                var bytes = reader.Reader.ReadBytes(1024 * 5);
                for (int i = 0; i < 1024*5; i++)
                {
                    Assert.Equal(2, bytes[i]);
                }
            }
        }
    }
}
