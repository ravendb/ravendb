using System;
using System.IO;
using Xunit;
using Voron;

namespace SlowTests.Voron
{
    public class BigValues : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

   
        [Fact]
        public void CanStoreInOneTransactionReallyBigValue()
        {
            var random = new Random(43321);
            var buffer = new byte[1024 * 1024 * 15 + 283];
            random.NextBytes(buffer);
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(tx.Allocator, BitConverter.GetBytes(1203), out key);
                tree.Add(key, new MemoryStream(buffer));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                Slice key;
                Slice.From(tx.Allocator, BitConverter.GetBytes(1203), out key);
                var readResult = tree.Read(key);
                Assert.NotNull(readResult);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
                Assert.Equal(buffer, memoryStream.ToArray());
                tx.Commit();
            }
        }
    }
}