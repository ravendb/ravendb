using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11827 : RavenTestBase
    {
        private class Data
        {
            public string Value { get; set; }
        }

        [Fact]
        public async Task CanIncrementOnNull()
        {
            var id = "data";
            using (var ds = GetDocumentStore())
            {
                using (var session = ds.OpenAsyncSession())
                {
                    await session.StoreAsync(new Data { Value = null }, id);
                    await session.SaveChangesAsync();
                }

                using (var session = ds.OpenAsyncSession())
                {
                    session.Advanced.Increment<Data, string>(id, d => d.Value, "something");
                    await session.SaveChangesAsync();
                }

                using (var session = ds.OpenAsyncSession())
                {
                    var data = await session.LoadAsync<Data>(id);
                    Assert.Equal("something", data.Value);
                }
            }
        }
    }
}
