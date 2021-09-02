using System;
using FastTests.Voron;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Issues
{
    public class RavenDB_15780 : StorageTest
    {
        public RavenDB_15780(ITestOutputHelper output) : base(output)
        {
        }

        [NightlyBuildTheory64Bit]
        [InlineData(10_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(100_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(1_000_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(10_000_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(10_000, 0.999, BloomFilterVersion.BaseVersion)]
        [InlineData(100_000, 0.940, BloomFilterVersion.BaseVersion)]
        [InlineData(1_000_000, 0.140, BloomFilterVersion.BaseVersion)]
        [InlineData(10_000, 0.999, null)]
        [InlineData(100_000, 0.940, null)]
        [InlineData(1_000_000, 0.140, null)]
        public void SuccessRate_BloomFilters_1(int count, double expectedSuccessRate, long? version)
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, 32 * 1024, SharedMultipleUseFlag.None))
            {
                var added = 0;

                SetUpCollectionOfBloomFilters(context, version);

                using (var tx = context.OpenWriteTransaction())
                {
                    using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        for (var i = 0; i < count; i++)
                        {
                            var key = context.GetLazyString($"orders/{i}");
                            if (filter.Add(key))
                                added++;
                        }
                    }
                }

                var successRate = added / (double)count;
                Assert.True(successRate >= expectedSuccessRate, $"{successRate} >= {expectedSuccessRate}");
            }
        }

        [NightlyBuildTheory64Bit]
        [InlineData(10_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(100_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(1_000_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(10_000_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(10_000, 0.999, BloomFilterVersion.BaseVersion)]
        [InlineData(100_000, 0.940, BloomFilterVersion.BaseVersion)]
        [InlineData(1_000_000, 0.140, BloomFilterVersion.BaseVersion)]
        [InlineData(10_000, 0.999, null)]
        [InlineData(100_000, 0.940, null)]
        [InlineData(1_000_000, 0.140, null)]
        public void SuccessRate_BloomFilters_2(int count, double expectedSuccessRate, long? version)
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, 32 * 1024, SharedMultipleUseFlag.None))
            {
                var added = 0;

                SetUpCollectionOfBloomFilters(context, version);

                using (var tx = context.OpenWriteTransaction())
                {
                    using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        for (var i = 0; i < count; i++)
                        {
                            var key = context.GetLazyString($"orders/{i:D19}-A");
                            if (filter.Add(key))
                                added++;
                        }
                    }
                }

                var successRate = added / (double)count;
                Assert.True(successRate >= expectedSuccessRate, $"{successRate} >= {expectedSuccessRate}");
            }
        }

        [NightlyBuildTheory64Bit]
        [InlineData(10_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(100_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(1_000_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(10_000_000, 0.999, BloomFilterVersion.CurrentVersion)]
        [InlineData(10_000, 0.999, BloomFilterVersion.BaseVersion)]
        [InlineData(100_000, 0.940, BloomFilterVersion.BaseVersion)]
        [InlineData(1_000_000, 0.140, BloomFilterVersion.BaseVersion)]
        [InlineData(10_000, 0.999, null)]
        [InlineData(100_000, 0.940, null)]
        [InlineData(1_000_000, 0.140, null)]
        public void SuccessRate_BloomFilters_3(int count, double expectedSuccessRate, long? version)
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, 32 * 1024, SharedMultipleUseFlag.None))
            {
                var added = 0;

                SetUpCollectionOfBloomFilters(context, version);

                using (var tx = context.OpenWriteTransaction())
                {
                    using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        for (var i = 0; i < count; i++)
                        {
                            var key = context.GetLazyString(Guid.NewGuid().ToString());
                            if (filter.Add(key))
                                added++;
                        }
                    }
                }

                var successRate = added / (double)count;
                Assert.True(successRate >= expectedSuccessRate, $"{successRate} >= {expectedSuccessRate}");
            }
        }

        [Fact]
        public void BloomFilter_Version()
        {
            using (var context = new TransactionOperationContext(Env, 1024, 1024, 32 * 1024, SharedMultipleUseFlag.None))
            {
                using (var tx = context.OpenWriteTransaction())
                {
                    using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        Assert.Equal(BloomFilterVersion.CurrentVersion, filter.Version);
                    }

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        Assert.Equal(BloomFilterVersion.CurrentVersion, filter.Version);
                    }

                    tx.Commit();
                }

                using (var tx = context.OpenWriteTransaction())
                {
                    using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        Assert.Equal(BloomFilterVersion.CurrentVersion, filter.Version);
                    }
                }

                SetVersion(context, BloomFilterVersion.BaseVersion);

                using (var tx = context.OpenWriteTransaction())
                {
                    using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        Assert.Equal(BloomFilterVersion.BaseVersion, filter.Version);
                    }
                }

                SetVersion(context, version: null);

                using (var tx = context.OpenWriteTransaction())
                {
                    using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                    {
                        Assert.Equal(BloomFilterVersion.BaseVersion, filter.Version);
                    }
                }
            }
        }

        private void SetVersion(TransactionOperationContext context, long? version)
        {
            using (var tx = context.OpenWriteTransaction())
            {
                var tree = tx.InnerTransaction.ReadTree("BloomFilters");

                if (version.HasValue)
                    tree.Add(CollectionOfBloomFilters.VersionSlice, version.Value);
                else
                    tree.Delete(CollectionOfBloomFilters.VersionSlice);

                tx.Commit();
            }

            using (var tx = context.OpenWriteTransaction())
            {
                using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                {
                    Assert.Equal(version ?? BloomFilterVersion.BaseVersion, filter.Version);
                }

                tx.Commit();
            }
        }

        private void SetUpCollectionOfBloomFilters(TransactionOperationContext context, long? version)
        {
            using (var tx = context.OpenWriteTransaction())
            {
                using (var filter = CollectionOfBloomFilters.Load(CollectionOfBloomFilters.Mode.X64, context))
                {
                }

                tx.Commit();
            }

            SetVersion(context, version);
        }
    }
}
