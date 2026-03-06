using System;
using System.IO;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Voron;

namespace SlowTests.Voron
{
    public class BigValues : StorageTest
    {
        public BigValues(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

   
        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Memory)]
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
                Assert.True(tree.TryRead(key, out var reader));

                var memoryStream = new MemoryStream();
                reader.CopyTo(memoryStream);
                Assert.Equal(buffer, memoryStream.ToArray());
                tx.Commit();
            }
        }
    }
}
