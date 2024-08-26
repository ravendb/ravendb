// -----------------------------------------------------------------------
//  <copyright file="ChecksumMismatchAfterRecovery.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Bugs
{
    public class ChecksumMismatchAfterRecovery : FastTests.Voron.StorageTest
    {
        public ChecksumMismatchAfterRecovery(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldNotThrowChecksumMismatch()
        {
            var random = new Random(1);
            var buffer = new byte[100];
            random.NextBytes(buffer);

            for (int i = 0; i < 100; i++)
            {
                buffer[i] = 13;
            }

            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 50; i++)
                    {
                        tree.Add("items/" + i, new MemoryStream(buffer));
                    }

                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 50; i < 100; i++)
                    {
                        tree.Add("items/" + i, new MemoryStream(buffer));
                    }

                    tx.Commit();
                }
            }

            options = StorageEnvironmentOptions.ForPathForTests(DataDir);

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.ReadTransaction())
                {
                    var tree = tx.CreateTree("foo");
                    for (int i = 0; i < 100; i++)
                    {
                        var readResult = tree.Read("items/" + i);
                        Assert.NotNull(readResult);
                        var memoryStream = new MemoryStream();
                        readResult.Reader.CopyTo(memoryStream);
                        Assert.Equal(memoryStream.ToArray(), buffer);
                    }
                }
            }
        }
    }
}
