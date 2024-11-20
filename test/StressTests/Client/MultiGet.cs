using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client
{
    public class MultiGet : RavenTestBase
    {
        public MultiGet(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task MultiGetCanGetFromCache()
        {
            var store = GetDocumentStore();

            using (var bulk = store.BulkInsert())
            {
                for (var i = 0; i < 10_000; i++)
                {
                    await bulk.StoreAsync(new User { Id = $"Users/{i}", Count = i });
                }
            }

            async Task Fetch()
            {
                for (var i = 0; i < 64; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var n1 = Random.Shared.Next(0, 10_000);
                        var n2 = Random.Shared.Next(0, 10_000);
                        session.Advanced.Lazily.LoadAsync<User>($"Users/{n1}");
                        session.Advanced.Lazily.LoadAsync<User>($"Users/{n2}");
                        await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();

                        var loaded1 = await session.LoadAsync<User>($"Users/{n1}");
                        Assert.Equal(n1, loaded1.Count);

                        var loaded2 = await session.LoadAsync<User>($"Users/{n2}");
                        Assert.Equal(n2, loaded2.Count);
                    }
                }
            }

            var tasks = new List<Task>();
            for (var i = 0; i < 100; i++)
            {
                tasks.Add(Task.Run(Fetch));
            }

            await Task.WhenAll(tasks);
        }
    }
}
