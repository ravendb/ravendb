using Sparrow;
using System;
using System.Collections.Generic;
using System.IO;
using Voron.Impl;
using Voron.Util;
using Xunit;
using Xunit.Extensions;

namespace Voron.Tests.Storage
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
                tree.Add(new Slice(BitConverter.GetBytes(1203)), new MemoryStream(buffer));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.CreateTree("foo");
                var readResult = tree.Read(new Slice(BitConverter.GetBytes(1203)));
                Assert.NotNull(readResult);

                var memoryStream = new MemoryStream();
                readResult.Reader.CopyTo(memoryStream);
                Assert.Equal(buffer, memoryStream.ToArray());
                tx.Commit();
            }
        }
    }
}