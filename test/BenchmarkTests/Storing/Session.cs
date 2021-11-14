using System;
using System.Threading.Tasks;
using BenchmarkTests.Utils;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace BenchmarkTests.Storing
{
    public class Session : BenchmarkTestBase
    {
        public Session(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Store_100k_Batch_Size_1()
        {
            using (var store = GetDocumentStore())
            {
                for (int i = 0; i < 100_000; i++)
                {
                    if (i % 10_000 == 0)
                        Console.WriteLine($"Inserted {i} documents");

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(EntityFactory.CreateCompanySmall(i));

                        await session.SaveChangesAsync();
                    }
                }
            }
        }

        [Fact]
        public async Task Store_500k_Batch_Size_10()
        {
            using (var store = GetDocumentStore())
            {
                for (int i = 0; i < 50_000; i++)
                {
                    if (i % 5_000 == 0)
                        Console.WriteLine($"Inserted {i * 10} documents");

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int j = 0; j < 10; j++)
                        {
                            await session.StoreAsync(EntityFactory.CreateCompanySmall(i, j));
                        }

                        await session.SaveChangesAsync();
                    }
                }
            }
        }

        [Fact]
        public async Task Store_1M_Batch_Size_100()
        {
            using (var store = GetDocumentStore())
            {
                for (int i = 0; i < 10_000; i++)
                {
                    if (i % 1_000 == 0)
                        Console.WriteLine($"Inserted {i * 100} documents");

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int j = 0; j < 100; j++)
                        {
                            await session.StoreAsync(EntityFactory.CreateCompanySmall(i, j));
                        }

                        await session.SaveChangesAsync();
                    }
                }
            }
        }

        public override Task InitAsync(DocumentStore store, string dbNamePostfix = "", Options options = null, int count = 1_000_000)
        {
            return Task.CompletedTask;
        }
    }
}
