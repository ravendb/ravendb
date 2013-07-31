using System;
using System.IO;
using Nevar.Impl;
using Xunit;

namespace Nevar.Tests.Storage
{
    public class MultiTransactions
    {
        [Fact]
        public void ShouldWork()
        {
            using (var env = new StorageEnvironment(new PureMemoryPager()))
            {
                for (int x = 0; x < 10*1000; x++)
                {
                    using (var tx = env.NewTransaction(TransactionFlags.ReadWrite))
                    {
                        var value = new byte[100];
                        new Random().NextBytes(value);
                        var ms = new MemoryStream(value);
                        for (long i = 0; i < (1000*1000) / (10*1000); i++)
                        {
                            ms.Position = 0;
                            env.Root.Add(tx, (x * i).ToString("0000000000000000"), ms);
                        }

                        tx.Commit();
                    }
                }
            }
        }
    }
}