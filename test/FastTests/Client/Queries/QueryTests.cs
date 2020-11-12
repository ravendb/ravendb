using System;
using System.Threading.Tasks;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client.Queries
{
    public class QueryTests : RavenTestBase
    {
        public QueryTests(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public async Task Query_WhenUsingDateTimeNowInWhereClause_ShouldSendRequestForEachQuery()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                const int numberOfRequests = 2;
                for (var i = 0; i < numberOfRequests; i++)
                {
                    _ = await session.Query<Order>()
                        .Where(x => x.OrderedAt < DateTime.Now)
                        .Take(5)
                        .ToListAsync();
                }

                Assert.Equal(numberOfRequests, session.Advanced.NumberOfRequests);
            }
        }
    }
}