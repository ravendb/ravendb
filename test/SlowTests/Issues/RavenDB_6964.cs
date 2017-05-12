using System;
using System.IO;
using FastTests.Voron;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6964 : StorageTest
    {
        [Fact]
        public void ShouldProperlyShrinkOverflowPageOnStreamAdd()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("Tree1");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                for (int i = 0; i < 10; i++)
                {
                    var tree = tx.ReadTree("Tree1");
                    var random = new Random();
                    var bytes = new byte[32 * 1024 * 1024];

                    random.NextBytes(bytes);

                    using (var s = new MemoryStream(bytes))
                        tree.AddStream("key/1", s);

                    tree.DeleteStream("key/1");
                }
            }
        }
    }
}