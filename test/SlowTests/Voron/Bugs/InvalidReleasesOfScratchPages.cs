using System.IO;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class InvalidReleasesOfScratchPages : FastTests.Voron.StorageTest
    {
        public InvalidReleasesOfScratchPages(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ReadTransactionCanReadJustCommittedValue()
        {
            var options = StorageEnvironmentOptions.CreateMemoryOnly();
            options.ManualFlushing = true;
            using (var env = new StorageEnvironment(options))
            {
                CreateTrees(env, 1, "tree");

                using (var txw = env.WriteTransaction())
                {
                    txw.CreateTree("tree0").Add("key/1", new MemoryStream());
                    txw.Commit();

                    using (var txr = env.ReadTransaction())
                    {
                        Assert.NotNull(txr.CreateTree("tree0").Read("key/1"));
                    }
                }
            }
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize *= 2;
        }

        [Fact]
        public void AllScratchPagesShouldBeReleased()
        {
            var options = StorageEnvironmentOptions.CreateMemoryOnly();
            options.ManualFlushing = true;
            using (var env = new StorageEnvironment(options))
            {
                using (var txw = env.WriteTransaction())
                {
                    txw.CreateTree("test");

                    txw.Commit();
                }

                using (var txw = env.WriteTransaction())
                {
                    var tree = txw.CreateTree("test");

                    tree.Add("key/1", new MemoryStream(new byte[100]));
                    tree.Add("key/1", new MemoryStream(new byte[200]));
                    txw.Commit();
                }

                env.FlushLogToDataFile(); // non read nor write transactions, so it should flush and release everything from scratch

                Assert.Equal(0, env.ScratchBufferPool.GetNumberOfAllocations(0)); 
            }
        }
    }
}
