using System;
using System.IO;
using FastTests.Voron.FixedSize;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Streams
{
    public class CanUseStream : FastTests.Voron.StorageTest
    {
        public CanUseStream(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(null, 129)]
        [InlineData(null, 2095)]
        [InlineData("AM4#@dF5Tas", 4096)]
        [InlineData(null, 8120)]
        [InlineData(null, 12004)]
        [InlineData("dfgja83mt7s", 15911)]
        [InlineData(null, 16897)]
        [InlineData(null, 31911)]
        [InlineData("fdd93m34nghdsya", 91911)]
        [InlineDataWithRandomSeed(null)]
        [InlineDataWithRandomSeed("RavenDB")]
        public void CanWriteAndRead(string tag, int size)
        {
            var buffer = new byte[size % 100000];
            new Random(size).NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer), tag);
                tx.Commit();
            }
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("Files");
                var readStream = tree.ReadStream("test");
                Assert.NotNull(readStream);
                for (int i = 0; i < buffer.Length; i++)
                {
                    Assert.Equal(buffer[i], readStream.ReadByte());
                }
                Assert.Equal(-1, readStream.ReadByte());

                var readTag = tree.GetStreamTag("test");

                if (tag == null)
                    Assert.Null(readTag);
                else
                    Assert.Equal(tag, readTag);
            }
        }
        [Theory]
        [InlineData(129)]
        [InlineData(2095)]
        [InlineData(4096)]
        [InlineData(8120)]
        [InlineData(12004)]
        [InlineData(15911)]
        [InlineData(16897)]
        [InlineData(31911)]
        [InlineData(91911)]
        [InlineDataWithRandomSeed]
        public void CanCopyTo(int size)
        {
            var buffer = new byte[size % 100000];
            new Random(size).NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer));
                tx.Commit();
            }
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("Files");
                var readStream = tree.ReadStream("test");
                Assert.NotNull(readStream);

                var memoryStream = new MemoryStream();
                readStream.CopyTo(memoryStream);

                Assert.Equal(buffer, memoryStream.ToArray());

                readStream.Position = 0;
                memoryStream.SetLength(0);
                readStream.CopyTo(memoryStream);

                Assert.Equal(buffer, memoryStream.ToArray());
            }
        }

        [Theory]
        [InlineData("AM4#@dF5Tas", 129)]
        [InlineData(null, 2095)]
        [InlineData(null, 4096)]
        [InlineData("AM4#@dF5Tas", 8120)]
        [InlineData("dfgja83mt7s", 12004)]
        [InlineData(null, 15911)]
        [InlineData(null, 16897)]
        [InlineData("fdd93m34nghdsya", 31911)]
        [InlineData(null, 91911)]
        [InlineDataWithRandomSeed(null)]
        [InlineDataWithRandomSeed("no sql")]
        public void CanUpdate(string tag, int size)
        {
            var buffer = new byte[size % 100000];
            new Random(size).NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer));
                tx.Commit();
            }
            new Random().NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer), tag);
                tx.Commit();
            }
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("Files");
                var readStream = tree.ReadStream("test");
                Assert.NotNull(readStream);
                for (int i = 0; i < buffer.Length; i++)
                {
                    Assert.Equal(buffer[i], readStream.ReadByte());
                }
                Assert.Equal(-1, readStream.ReadByte());

                var readTag = tree.GetStreamTag("test");

                if (tag == null)
                    Assert.Null(readTag);
                else
                    Assert.Equal(tag, readTag);
            }
        }

        [Theory]
        [InlineData(null, 50)]
        [InlineData(null, 129)]
        [InlineData(null, 2095)]
        [InlineData("debug tag", 4096)]
        [InlineData(null, 8120)]
        [InlineData("tag", 12004)]
        [InlineData(null, 15911)]
        [InlineData(null, 16897)]
        [InlineData("Debug debug tag", 31911)]
        [InlineData(null, 91911)]
        [InlineDataWithRandomSeed(null)]
        public void CanDelete(string tag, int size)
        {
            var buffer = new byte[size % 100000];
            new Random(size).NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer), tag);
                tx.Commit();
            }
            new Random().NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                Assert.NotEqual(0, tree.DeleteStream("test"));
                tx.Commit();
            }
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("Files");
                var readStream = tree.ReadStream("test");
                Assert.Null(readStream);
            }
        }

        [Theory]
        [InlineData("RavenDB", 1546581643)]
        [InlineDataWithRandomSeed(null)]
        [InlineDataWithRandomSeed("RavenDB")]
        public void TreeShouldReturnAllPagesOccupiedByStreams_RavenDB_5990(string tag, int size)
        {
            var buffer = new byte[size % 100000];
            new Random(size).NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("test", new MemoryStream(buffer), tag);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Files");

                using (Slice.From(tx.Allocator, "test", out var fileName))
                {
                    Assert.Equal(tree.State.Header.OverflowPages + tree.State.Header.BranchPages + tree.State.Header.LeafPages, tree.AllPages().Count);
                }
            }
        }
    }
}
