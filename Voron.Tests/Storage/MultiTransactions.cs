using System;
using System.IO;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Storage
{
    public class MultiTransactions
    {
        [Fact]
        public void ShouldWork()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            {
                for (int x = 0; x < 10; x++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var value = new byte[100];
                        new Random().NextBytes(value);
                        var ms = new MemoryStream(value);
                        for (long i = 0; i < 100; i++)
                        {
                            ms.Position = 0;
                            
                            tx.State.Root.Add(tx, (x * i).ToString("0000000000000000"), ms);
                        }

                        tx.Commit();
                    }
                }
            }
        }
    }
}