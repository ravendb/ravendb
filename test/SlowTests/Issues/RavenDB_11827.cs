using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11827 : RavenTestBase
    {
        public RavenDB_11827(ITestOutputHelper output) : base(output)
        {
        }

        private class Data
        {
            public string Value { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public async Task CanIncrementOnNull(Options options)
        {
            var id = "data";
            using (var ds = GetDocumentStore(options))
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
