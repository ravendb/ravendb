using System.Threading.Tasks;
using BenchmarkTests.Utils;
using Raven.Client.Documents;
using Xunit;

namespace BenchmarkTests.Storing
{
    public class BulkInsert : BenchmarkTestBase
    {
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

        private async Task Small_Size_Internal(int count)
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < count; i++)
                    {
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
