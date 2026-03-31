using System;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues
{
    public unsafe class RavenDB_26285 : StorageTest
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

        [RavenFact(RavenTestCategory.Voron)]
        public void CanDeleteInlineStream()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/1", new MemoryStream(new byte[128]));
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("Streams");
                using (Slice.From(tx.Allocator, "stream/1", out Slice key))
                {
                    Assert.True(tree.StreamExist(key));
                    tree.DeleteStream(key);
                    Assert.False(tree.StreamExist(key));
                }
                tx.Commit();
            }

            // Verify it stays deleted after commit
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                Assert.Null(tree.ReadStream("stream/1"));
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanUpdateInlineStream()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/1", new MemoryStream(new byte[64]));
                tx.Commit();
            }

            // Overwrite with different data
            var newData = new byte[100];
            new Random(42).NextBytes(newData);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("Streams");
                tree.AddStream("stream/1", new MemoryStream(newData));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                using var stream = tree.ReadStream("stream/1");
                Assert.NotNull(stream);

                var readBack = new byte[100];
                var bytesRead = stream.Read(readBack, 0, readBack.Length);
                Assert.Equal(newData.Length, bytesRead);
                Assert.Equal(newData, readBack);
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1)]
        [InlineData(16)]
        [InlineData(64)]
        [InlineData(128)]
        [InlineData(256)]
        [InlineData(512)]
        [InlineData(1024)]
        [InlineData(2048)]
        public void InlineStream_VariousSizes(int size)
        {
            var data = new byte[size];
            new Random(size).NextBytes(data);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/1", new MemoryStream(data));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");

                using var stream = tree.ReadStream("stream/1");
                Assert.NotNull(stream);
                Assert.Equal(size, stream.Length);

                var readBack = new byte[size];
                var totalRead = 0;
                while (totalRead < size)
                {
                    var read = stream.Read(readBack, totalRead, size - totalRead);
                    if (read == 0)
                        break;
                    totalRead += read;
                }

                Assert.Equal(size, totalRead);
                Assert.Equal(data, readBack);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void TouchStream_WorksForInlineStreams()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/1", new MemoryStream(new byte[128]));
                tx.Commit();
            }

            int version;
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("Streams");
                using (Slice.From(tx.Allocator, "stream/1", out Slice key))
                    version = tree.TouchStream(key);
                Assert.True(version > 0);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("Streams");
                using (Slice.From(tx.Allocator, "stream/1", out Slice key))
                {
                    var version2 = tree.TouchStream(key);
                    Assert.Equal(version + 1, version2);
                }
                tx.Commit();
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void StorageReport_IncludesInlineStreams()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/1", new MemoryStream(new byte[64]));
                tree.AddStream("stream/2", new MemoryStream(new byte[128]));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);
                var streamsTree = report.Trees.FirstOrDefault(t => t.Name == "Streams");
                Assert.NotNull(streamsTree);
                Assert.NotNull(streamsTree.Streams);
                Assert.Equal(2, streamsTree.Streams.NumberOfStreams);
                Assert.Equal(2, streamsTree.Streams.Streams.Count);

                // Inline streams should not allocate overflow pages
                foreach (var stream in streamsTree.Streams.Streams)
                {
                    Assert.Equal(0, stream.NumberOfAllocatedPages);
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CanMixInlineAndChunkBasedStreams()
        {
            var smallData = new byte[64];
            var largeData = new byte[64 * 1024]; // 64KB - too large for inline
            new Random(1).NextBytes(smallData);
            new Random(2).NextBytes(largeData);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/small", new MemoryStream(smallData));
                tree.AddStream("stream/large", new MemoryStream(largeData));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");

                using (Slice.From(tx.Allocator, "stream/small", out Slice smallKey))
                using (Slice.From(tx.Allocator, "stream/large", out Slice largeKey))
                {
                    Assert.True(tree.StreamExist(smallKey));
                    Assert.True(tree.StreamExist(largeKey));

                    // Verify small is inline
                    Assert.True(tree.IsInlineStream("stream/small", out _, out _, out _));
                    Assert.False(tree.IsInlineStream("stream/large", out _, out _, out _));
                }

                // Verify data integrity for both
                using var smallStream = tree.ReadStream("stream/small");
                var smallReadBack = new byte[64];
                var read = smallStream.Read(smallReadBack, 0, smallReadBack.Length);
                Assert.Equal(64, read);
                Assert.Equal(smallData, smallReadBack);

                using var largeStream = tree.ReadStream("stream/large");
                using var ms = new MemoryStream();
                largeStream.CopyTo(ms);
                Assert.Equal(largeData, ms.ToArray());
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void StorageReport_MixedInlineAndChunkStreams()
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/small", new MemoryStream(new byte[64]));
                tree.AddStream("stream/large", new MemoryStream(new byte[64 * 1024]));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var report = Env.GenerateDetailedReport(tx, includeDetails: true);
                var streamsTree = report.Trees.FirstOrDefault(t => t.Name == "Streams");
                Assert.NotNull(streamsTree);
                Assert.NotNull(streamsTree.Streams);
                Assert.Equal(2, streamsTree.Streams.NumberOfStreams);

                var small = streamsTree.Streams.Streams.FirstOrDefault(s => s.Length == 64);
                var large = streamsTree.Streams.Streams.FirstOrDefault(s => s.Length == 64 * 1024);

                Assert.NotNull(small);
                Assert.NotNull(large);
                Assert.Equal(0, small.NumberOfAllocatedPages);
                Assert.True(large.NumberOfAllocatedPages > 0);
            }
        }
    }
}
