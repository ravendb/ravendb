using System;
using System.Threading.Tasks;
using BenchmarkTests.Utils;
using Raven.Client.Documents;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace BenchmarkTests.Storing
{
    public class BulkInsert : BenchmarkTestBase
    {
        public BulkInsert(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Small_Size_500k()
        {
            await Small_Size_Internal(500_000);
        }

        [Fact]
        public async Task Small_Size_1M()
        {
            await Small_Size_Internal(1_000_000);
        }

        [Fact]
        public async Task Large_Size_500k()
        {
            await Large_Size_Internal(500_000);
        }

        [Fact]
        public async Task Large_Size_1M()
        {
            await Large_Size_Internal(1_000_000);
        }

        [Fact]
        public async Task VeryBigTransactionsForBulkInsertOfCounters()
        {
            using (var store = GetDocumentStore())
            {
                const string baseDocId = "users/";
                using (var bulk = store.BulkInsert())
                {
                    var docId = baseDocId + 1;
                    await bulk.StoreAsync(new User() { Name = "Grisha" }, docId);

                    var counters = bulk.CountersFor(docId);
                    for (var j = 0; j < 1_000_000; j++)
                    {
                        var rnd = new Random(DateTime.Now.Millisecond);
                        await counters.IncrementAsync(j.ToString(), rnd.Next(1, 1_000_000));
                    }
                }
            }
        }

        [Fact]
        public async Task VeryBigTransactionsForBulkInsertOfTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                const string baseDocId = "users/";
                using (var bulk = store.BulkInsert())
                {
                    for (var i = 1; i < 1_000; i++)
                    {
                        var docId = baseDocId + 1;
                        await bulk.StoreAsync(new User() { Name = "Grisha" }, docId);

                        using var counters = bulk.TimeSeriesFor(docId, $"test_{i}");
                        for (var j = 0; j < 100_000; j++)
                        {
                            var rnd = new Random(DateTime.Now.Millisecond);
                            await counters.AppendAsync(DateTime.Now.AddMilliseconds(j), rnd.Next(1, 1_000_000));
                        }
                    }
                }
            }
        }

        private async Task Small_Size_Internal(int count)
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (i % 10_000 == 0)
                            Console.WriteLine($"Inserted {i} documents");

                        await bulkInsert.StoreAsync(EntityFactory.CreateCompanySmall(i));
                    }
                }
            }
        }

        private async Task Large_Size_Internal(int count)
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
                        if (i % 10_000 == 0)
                            Console.WriteLine($"Inserted {i} documents");

                        await bulkInsert.StoreAsync(EntityFactory.CreateCompanyLarge(i));
                    }
                }
            }
        }

        public override Task InitAsync(DocumentStore store)
        {
            return Task.CompletedTask;
        }
    }
}
