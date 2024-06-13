using System;
using System.Collections.Generic;
using System.IO;
using Sparrow;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Storage
{
    public class BigValues : FastTests.Voron.StorageTest
    {
        public BigValues(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void CanReuseLargeSpace(int restartCount)
        {
            var random = new Random(43321);
            var buffer = new byte[1024 * 1024 * 6 + 283];
            random.NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                tree.Add(key, new MemoryStream(buffer));
                tx.Commit();
            }

            if (restartCount >= 1)
                RestartDatabase();

            Env.FlushLogToDataFile();

            var old = Env.CurrentStateRecord.DataPagerState.NumberOfAllocatedPages;

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                tree.Delete(key);
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                var readResult = tree.Read(key);
                Assert.Null(readResult);
            }

            if (restartCount >= 2)
                RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                var readResult = tree.Read(key);
                Assert.Null(readResult);
            }

            using (var tx = Env.WriteTransaction())
            {
                buffer = new byte[1024 * 1024 * 3 + 1238];
                random.NextBytes(buffer);
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                tree.Add(key, new MemoryStream(buffer));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                var readResult = tree.Read(key);
                Assert.NotNull(readResult);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
                CompareBuffers(buffer, memoryStream);
                tx.Commit();
            }

            if (restartCount >= 3)
                RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                var readResult = tree.Read(key);
                Assert.NotNull(readResult);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
                CompareBuffers(buffer, memoryStream);
                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                var readResult = tree.Read(key);
                Assert.NotNull(readResult);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
                CompareBuffers(buffer, memoryStream);
                tx.Commit();
            }

            if (restartCount >= 4)
                RestartDatabase();

            Assert.Equal(old, Env.CurrentStateRecord.DataPagerState.NumberOfAllocatedPages);

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(Allocator, BitConverter.GetBytes(1203), out key);
                var readResult = tree.Read(key);
                Assert.NotNull(readResult);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
                CompareBuffers(buffer, memoryStream);
                tx.Commit();
            }
        }

        private static unsafe void CompareBuffers(byte[] buffer, MemoryStream memoryStream)
        {
            fixed (byte* b = buffer)
            fixed (byte* c = memoryStream.ToArray())
                Assert.Equal(0, Memory.Compare(b, c, buffer.Length));
        }

        [Fact]
        public void CanStoreInOneTransactionManySmallValues()
        {
            var buffers = new List<byte[]>();
            var random = new Random(43321);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 1500; i++)
                {
                    var buffer = new byte[912];
                    random.NextBytes(buffer);
                    buffers.Add(buffer);
                    Slice key;
                    Slice.From(Allocator, BitConverter.GetBytes(i), out key);
                    tree.Add(key, new MemoryStream(buffer));
                }
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                for (int i = 0; i < 1500; i++)
                {
                    Slice key;
                    Slice.From(Allocator, BitConverter.GetBytes(i), out key);
                    var readResult = tree.Read(key);
                    Assert.NotNull(readResult);

                    var memoryStream = new MemoryStream();
                    readResult.Reader.CopyTo(memoryStream);
                    Assert.Equal(buffers[i], memoryStream.ToArray());

                }
                tx.Commit();
            }
        }
    }
}
