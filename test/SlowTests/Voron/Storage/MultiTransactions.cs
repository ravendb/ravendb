﻿using System;
using System.IO;
using FastTests;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Storage
{
    public class MultiTransactions : NoDisposalNeeded
    {
        public MultiTransactions(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ShouldWork()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnlyForTests()))
            {
                for (int x = 0; x < 10; x++)
                {
                    using (var tx = env.WriteTransaction())
                    {
                        var tree = tx.CreateTree("foo");
                        var value = new byte[100];
                        new Random().NextBytes(value);
                        var ms = new MemoryStream(value);
                        for (long i = 0; i < 100; i++)
                        {
                            ms.Position = 0;

                            tree.Add((x * i).ToString("0000000000000000"), ms);
                        }

                        tx.Commit();
                    }
                }
            }
        }
    }
}
