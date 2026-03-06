using System;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron
{
    public class SplittingVeryBig : StorageTest
    {
        public SplittingVeryBig(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxLogFileSize = 10 * Constants.Storage.PageSize;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ShouldBeAbleToWriteValuesGreaterThanLogAndReadThem()
        {
            var random = new Random(1234);
            var buffer = new byte[1024 * 512];
            random.NextBytes(buffer);

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree");
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("key1", new MemoryStream(buffer));
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Assert.True(tx.CreateTree("tree").TryRead("key1", out var reader));
                Assert.Equal(buffer.Length, reader.Length);
                
                var bytes = reader.ReadBytes(reader.Length);
                Assert.Equal(buffer, bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray());
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void ShouldBeAbleToWriteValuesGreaterThanLogAndRecoverThem()
        {
            var random = new Random(1234);
            var buffer = new byte[1024 * 512];
            random.NextBytes(buffer);

            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            options.MaxLogFileSize = 10 * Constants.Storage.PageSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("tree");
                    tx.Commit();
                }

                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("key1", new MemoryStream(buffer));
                    tx.Commit();
                }
            }

            options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            options.MaxLogFileSize = 10 * Constants.Storage.PageSize;

            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("tree");
                    tx.Commit();
                }

                using (var tx = env.ReadTransaction())
                {
                    Assert.True(tx.CreateTree("tree").TryRead("key1", out var reader));
                    {
                        Assert.Equal(buffer.Length, reader.Length);
                        var bytes = reader.ReadBytes(reader.Length);
                        Assert.Equal(buffer, bytes.Array.Skip(bytes.Offset).Take(bytes.Count).ToArray());

                    }
                }
            }
        }
    }
}
