using System;
using System.IO;
using Sparrow.Logging;
using Xunit;
using Voron;

namespace FastTests.Voron.Storage
{
    public class MultiTransactions
    {
        private static readonly LoggerSetup NullLoggerSetup = new LoggerSetup(System.IO.Path.GetTempPath(), LogMode.None);

        [Fact]
        public void ShouldWork()
        {
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly(), NullLoggerSetup))
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
