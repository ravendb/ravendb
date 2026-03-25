using System.IO;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_26285 : StorageTest
    {
        public RavenDB_26285(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void SmallStreams_ShouldBeStoredInline_WithoutAllocatingNewPages()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("Streams");
                tx.Commit();
            }

            var pagesBefore = Env.NextPageNumber;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("Streams");
                var data = new byte[128];

                for (int i = 0; i < 10; i++)
                {
                    tree.AddStream($"stream/{i}", new MemoryStream(data));
                }

                tx.Commit();
            }

            var pagesAfter = Env.NextPageNumber;

            Assert.Equal(pagesBefore, pagesAfter);
        }
    }
}
