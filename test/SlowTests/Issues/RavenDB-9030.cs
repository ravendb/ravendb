using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9030: RavenTestBase
    {
        public RavenDB_9030(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void InQueryOnMultipleIdsShouldNotThrowTooManyBooleanClauses()
        {
            var numOfIds = 10_000;
            var ids = Enumerable.Range(0, numOfIds).Select(x => x.ToString()).ToArray();
            using (var store = GetDocumentStore())
            {
              
                using (var session = store.OpenSession())
                {
                    Assert.Equal(0, session.Query<Document>()
                        .Where(x => x.Id.In(ids))
                        .Select(x => new
                        {
                            x.Id
                        }).Count());
                }
            }
        }

        public class Document
        {
            public string Id { get; set; }
        }
    }
}
