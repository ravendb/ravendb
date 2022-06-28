using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18664 : RavenTestBase
    {
        public RavenDB_18664(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GivenADocument_WhenAnEmptyListIsPassedToCheckIfIdsExist_QueryShouldReturnZeroResults()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    CreateTestDocument(session);
                    session.SaveChanges();
                }

                var emptyList = new List<string>();

                using (var session = store.OpenSession())
                {
                    var queryCount = session.Query<TestDocument>().Count(x => x.Id.In(emptyList));
                    Assert.Equal(0, queryCount);
                }
            }
        }

        private static void CreateTestDocument(IDocumentSession session)
        {
            var testDoc = new TestDocument
            {
                Comment = "TestDoc1"
            };

            session.Store(testDoc);
        }

        private class TestDocument
        {
            public string Id { get; set; }
            public string Comment { get; set; }
        }
    }
}
