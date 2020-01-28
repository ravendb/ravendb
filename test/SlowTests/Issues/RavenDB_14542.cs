using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14542 : RavenTestBase
    {
        public RavenDB_14542(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void QueryShouldQuoteCollectionName()
        {
            using (var store = GetDocumentStore())
            {
                Validate(store, "Core/Order", "Core/Order");
                Validate(store, "1234", "1234");
            }
        }

        private void Validate(IDocumentStore store, string collectionName, string expectedCollectionName)
        {
            using (var session = store.OpenSession())
            {
                var query = session
                    .Query<Order>(collectionName: collectionName);

                var queryAsString = query.ToString();

                Assert.Equal($"from '{expectedCollectionName}'", queryAsString);

                var results = query.ToList(); // checking if this will be parsed by the RQL parser

                Assert.Equal(0, results.Count);
            }
        }
    }
}
