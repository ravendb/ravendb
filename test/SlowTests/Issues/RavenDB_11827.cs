using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
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
        [JavaScriptEngineClassData]
        public async Task CanIncrementOnNull(string jsEngineType)
        {
            var id = "data";
            using (var ds = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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
