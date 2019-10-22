using FastTests;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8320 : RavenTestBase
    {
        public RavenDB_8320(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void DisableCachingShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    var query = new IndexQuery
                    {
                        Query = "FROM Users"
                    };

                    var result = commands.Query(query);
                    Assert.True(result.DurationInMs >= 0);
                    Assert.Equal(1, commands.RequestExecutor.Cache.NumberOfItems);

                    result = commands.Query(query);
                    Assert.Equal(-1, result.DurationInMs);

                    query.DisableCaching = true;
                    result = commands.Query(query);
                    Assert.True(result.DurationInMs >= 0);

                    commands.RequestExecutor.Cache.Clear();
                    Assert.Equal(0, commands.RequestExecutor.Cache.NumberOfItems);

                    query.DisableCaching = true;
                    result = commands.Query(query);
                    Assert.True(result.DurationInMs >= 0);
                    Assert.Equal(0, commands.RequestExecutor.Cache.NumberOfItems);
                }
            }
        }
    }
}
