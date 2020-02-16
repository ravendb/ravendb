using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB14589 : RavenTestBase
    {
        public RavenDB14589(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanGetUpdatesCounterValueUsingInclude()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new { }, "users/1");
                await s.SaveChangesAsync();
            }

            for (int i = 0; i < 3; i++)
            {
                using var s = store.OpenAsyncSession();
                var doc = await s.LoadAsync<object>("users/1", 
                    include => include.IncludeCounter("requests"));
                var val = await s.CountersFor(doc).GetAsync("requests") ?? 0;
                Assert.Equal(i, val);
                s.CountersFor(doc).Increment("requests");
                await s.SaveChangesAsync();
            }
        }

        [Fact]
        public async Task CanGetUpdatesCounterValueUsingInclude_UsingQuery()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenAsyncSession())
            {
                await s.StoreAsync(new { Active = true }, "users/1");
                await s.SaveChangesAsync();
            }

            for (int i = 0; i < 3; i++)
            {
                using var s = store.OpenAsyncSession();
                // collection query
                var docs = await s.Advanced.AsyncRawQuery<object>(
                        @"from @all_docs include counters('requests')")
                    .ToListAsync();
                foreach (var doc in docs)
                {
                    var val = await s.CountersFor(doc).GetAsync("requests") ?? 0;
                    Assert.Equal(i, val);
                    s.CountersFor(doc).Increment("requests");
                    await s.SaveChangesAsync();
                }
            }

            for (int i = 0; i < 3; i++)
            {
                using var s = store.OpenAsyncSession();
                // non-collection query
                var docs = await s.Advanced.AsyncRawQuery<object>(
                        @"from @all_docs where Active = true include counters('requests2')")
                    .ToListAsync();
                foreach (var doc in docs)
                {
                    var val = await s.CountersFor(doc).GetAsync("requests2") ?? 0;
                    Assert.Equal(i, val);
                    s.CountersFor(doc).Increment("requests2");
                    await s.SaveChangesAsync();
                }
            }
        }
    }
}
