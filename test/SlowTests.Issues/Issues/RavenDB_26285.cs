using System;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Raven.Server.Indexing;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl;
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

        [RavenFact(RavenTestCategory.Voron)]
        public void UnmanagedVoronStream_PositionClamping()
        {
            // Test that UnmanagedVoronStream safely clamps Position to valid range [0, Length]
            // when callers set it beyond bounds. This is used internally for inline stream reading.

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                var data = new byte[256];
                new Random(42).NextBytes(data);
                tree.AddStream("stream/1", new MemoryStream(data));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                using var stream = tree.ReadStream("stream/1");

                // Reading to end should work
                var buffer = new byte[256];
                var read = stream.Read(buffer, 0, 256);
                Assert.Equal(256, read);

                // Setting position beyond length should clamp to length
                stream.Position = 500;
                Assert.Equal(256, stream.Position);

                // Setting negative position should clamp to 0
                stream.Position = -10;
                Assert.Equal(0, stream.Position);

                // Seeking should work correctly
                stream.Seek(100, SeekOrigin.Begin);
                Assert.Equal(100, stream.Position);

                stream.Seek(-50, SeekOrigin.Current);
                Assert.Equal(50, stream.Position);

                stream.Seek(-100, SeekOrigin.End);
                Assert.Equal(156, stream.Position);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void InlineStream_CrossTransactionAccess()
        {
            // Tests cross-transaction access to inline streams, which exercises
            // the path that LuceneVoronStream.UpdateCurrentTransaction uses when
            // refreshing stream access across transaction boundaries.
            // This is the scenario where Lucene/Corax maintains per-thread readers
            // that cache stream pointers and need to refresh them on new transactions.

            var data = new byte[1024];
            new Random(99).NextBytes(data);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/1", new MemoryStream(data));
                tx.Commit();
            }

            // First transaction: read and cache position
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                using var stream = tree.ReadStream("stream/1");
                stream.Position = 100;
                Assert.Equal(100, stream.Position);

                var buffer = new byte[100];
                var read = stream.Read(buffer, 0, 100);
                Assert.Equal(100, read);
            }

            // Second transaction: read again from scratch
            // This simulates Lucene needing to refresh its stream accessor
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                using var stream = tree.ReadStream("stream/1");

                // Position should start fresh
                Assert.Equal(0, stream.Position);

                // But we should still be able to read the same data
                var buffer = new byte[1024];
                var read = stream.Read(buffer, 0, 1024);
                Assert.Equal(1024, read);
                Assert.Equal(data, buffer);
            }

            // Third transaction: seek and read
            // Further exercise the cross-transaction robustness
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                using var stream = tree.ReadStream("stream/1");

                stream.Seek(500, SeekOrigin.Begin);
                Assert.Equal(500, stream.Position);

                var buffer = new byte[24];
                var read = stream.Read(buffer, 0, 24);
                Assert.Equal(24, read);
                Assert.Equal(data.Skip(500).Take(24).ToArray(), buffer);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void StreamSizeTransition_SimulatesV24Upgrade()
        {
            // This test simulates the v23→v24 upgrade scenario where a large chunked stream
            // becomes small enough to inline during compaction.
            // In reality: large data was stored chunked in v23, but after compaction only
            // the used portion remains (small), so it can be stored inline in v24.

            const int largeSize = 8192;  // Larger than inline limit
            const int tinySize = 128;    // Fits inline

            var largeData = new byte[largeSize];
            new Random(99).NextBytes(largeData);
            var tinyData = largeData.Take(tinySize).ToArray();

            // Step 1: Store large stream as chunked
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/transition", new MemoryStream(largeData));
                tx.Commit();
            }

            // Step 2: Verify it was stored as chunked
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                Assert.False(tree.IsInlineStream("stream/transition", out _, out _, out _),
                    "Stream should be chunked (8KB > inline limit)");
            }

            // Step 3: Simulate compaction: delete large and re-add with only used portion
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("Streams");
                tree.Delete("stream/transition");
                tree.AddStream("stream/transition", new MemoryStream(tinyData));
                tx.Commit();
            }

            // Step 4: Verify stream is now inline
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                Assert.True(tree.IsInlineStream("stream/transition", out _, out _, out _),
                    "After compaction with small data, stream should be inline");

                using var stream = tree.ReadStream("stream/transition");
                var buffer = new byte[tinySize];
                var bytesRead = stream.Read(buffer, 0, tinySize);
                Assert.Equal(tinySize, bytesRead);
                Assert.Equal(tinyData, buffer);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void InlineStreams_CrossTransactionAccessAfterRebind()
        {
            // Tests the cross-transaction refresh path for inline streams.
            // Single LuceneVoronStream instance persists across transaction boundaries:
            // - TX1: create instance and read
            // - Close TX1 (cleanup handler runs: nulls _llt, calls UpdatePtr(null))
            // - TX2: call UpdateCurrentTransaction to rebind to new transaction
            // - TX2: read from same instance
            // This simulates Lucene's per-thread reader cache outliving individual transactions.

            var tinyData = new byte[128];
            new Random(77).NextBytes(tinyData);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Files");
                tree.AddStream("tiny_file", new MemoryStream(tinyData));
                tx.Commit();
            }

            // TX1: Create a single LuceneVoronStream instance
            LuceneVoronStream cachedStream = null;

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Files");
                byte* inlinePtr = null;
                int inlineSize = 0;

                if (tree.IsInlineStream("tiny_file", out inlinePtr, out inlineSize, out _))
                {
                    // IsInlineStream returns pointer to header. Skip past header and tag to get actual data.
                    var header = (Tree.InlineStreamHeader*)inlinePtr;
                    byte* dataPtr = inlinePtr + Tree.InlineStreamHeader.SizeOf + header->Info.TagSize;
                    int dataSize = inlineSize - Tree.InlineStreamHeader.SizeOf - header->Info.TagSize;

                    // Create the instance (holds reference to TX1's LowLevelTransaction)
                    cachedStream = new LuceneVoronStream("tiny_file", "Files", dataPtr, dataSize, tx.LowLevelTransaction);

                    // Read partway to verify it works in TX1
                    byte[] readData = new byte[64];
                    var bytesRead = cachedStream.Read(readData, 0, 64);
                    Assert.Equal(64, bytesRead);
                    Assert.Equal(tinyData.Take(64).ToArray(), readData);
                }
            } // TX1 closes here: cleanup handler fires, _inlineStream._ptr = null

            // Verify we have the instance (it survived TX1 close)
            Assert.NotNull(cachedStream);

            // TX2: Rebind same instance to a new transaction
            using (var tx = Env.ReadTransaction())
            {
                // Rebind the cached stream to TX2
                cachedStream.UpdateCurrentTransaction(tx);

                // Now read from the same instance in TX2 (pointer was refreshed)
                var fullData = new byte[128];
                cachedStream.Position = 0;  // Reset position
                var bytesRead = cachedStream.Read(fullData, 0, 128);
                Assert.Equal(128, bytesRead);
                Assert.Equal(tinyData, fullData);
            }
        }

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(64)]          // fits inline
        [InlineData(2048)]        // fits inline (boundary-ish)
        [InlineData(8 * 1024)]    // exceeds inline, falls back to chunked - this is the embeddings regression path
        [InlineData(128 * 1024)]  // far exceeds inline, multiple chunks
        public void CanAddStream_WithNonSeekableStream(int size)
        {
            // Regression: embeddings generator passes a CanSeek=false stream; AddStream
            // used to assert seekability because the inline-probe fallback reset Position.
            var data = new byte[size];
            new Random(size).NextBytes(data);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream("stream/1", new NonSeekableStream(data));
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

        private sealed class NonSeekableStream : Stream
        {
            private readonly byte[] _data;
            private int _position;

            public NonSeekableStream(byte[] data)
            {
                _data = data;
            }

            public override bool CanRead => true;
            public override bool CanSeek => false;
            public override bool CanWrite => false;
            public override long Length => _data.Length;

            public override long Position
            {
                get => _position;
                set => throw new NotSupportedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                var remaining = _data.Length - _position;
                if (remaining <= 0)
                    return 0;

                var toRead = Math.Min(count, remaining);
                Buffer.BlockCopy(_data, _position, buffer, offset, toRead);
                _position += toRead;
                return toRead;
            }

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override void Flush() { }
        }
    }

    public unsafe class RavenDB_26285_Encrypted : StorageTest
    {
        public RavenDB_26285_Encrypted(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.Encryption.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        public void InlineStreamsWithEncryption()
        {
            // Tests inline stream storage with encryption enabled.
            // Inline stream data is stored directly in encrypted tree node pages.
            // This verifies that reads correctly decrypt and return the inline data.

            var smallData = new byte[256];
            new Random(42).NextBytes(smallData);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("EncryptedStreams");
                tree.AddStream("stream/1", new MemoryStream(smallData));
                tx.Commit();
            }

            // Verify inline stream can be read back correctly with encryption
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("EncryptedStreams");
                using var stream = tree.ReadStream("stream/1");
                Assert.NotNull(stream);
                Assert.Equal(smallData.Length, stream.Length);

                var readBack = new byte[smallData.Length];
                var bytesRead = stream.Read(readBack, 0, readBack.Length);
                Assert.Equal(smallData.Length, bytesRead);
                Assert.Equal(smallData, readBack);
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void InlineStreams_LongKeysStillGetInlined()
        {
            // Tests that maxInlineSize calculation doesn't incorrectly subtract key.Size.
            // Streams with longer keys should still be inlined if the value fits.
            // This verifies the fix for the size calculation bug.

            var smallData = new byte[512];
            new Random(55).NextBytes(smallData);

            // Use a very long key to test that it doesn't prevent inlining
            var longKey = "stream/" + new string('x', 500);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Streams");
                tree.AddStream(longKey, new MemoryStream(smallData));
                tx.Commit();
            }

            // Verify the stream was inlined despite the long key
            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("Streams");
                Assert.True(tree.IsInlineStream(longKey, out _, out _, out _),
                    "Small stream should be inlined even with long key");

                using var stream = tree.ReadStream(longKey);
                Assert.NotNull(stream);
                Assert.Equal(smallData.Length, stream.Length);

                var readBack = new byte[smallData.Length];
                var bytesRead = stream.Read(readBack, 0, readBack.Length);
                Assert.Equal(smallData.Length, bytesRead);
                Assert.Equal(smallData, readBack);
            }
        }
    }
}
