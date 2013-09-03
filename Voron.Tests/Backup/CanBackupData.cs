using System;
using System.IO;
using System.Threading.Tasks;
using Voron.Impl;
using Xunit;

namespace Voron.Tests.Backup
{
    public class CanBackupData : StorageTest
    {
        [Fact]
        public void ToStream()
        {
            var random = new Random();
            var buffer = new byte[8192];
            random.NextBytes(buffer);
            using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
            {
                Env.Root.Add(tx, "a", new MemoryStream(buffer));
                tx.Commit();
            }

            var stream = new MemoryStream();
            Env.Backup(stream);


            using (var pureMemoryPager = new PureMemoryPager(stream.ToArray()))
            using (var env = new StorageEnvironment(pureMemoryPager, false))
            {
                using (var tx = env.NewTransaction(TransactionFlags.Read))
                {
                    var readResult = env.Root.Read(tx, "a");
                    Assert.NotNull(readResult);
                    var memoryStream = new MemoryStream();
                    readResult.Stream.CopyTo(memoryStream);
                    Assert.Equal(memoryStream.ToArray(), buffer);
                }
            }
        }
    }
}