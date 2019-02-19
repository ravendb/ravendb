using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Bugs
{
    public class RavenDB9939 : RavenTestBase
    {
        [Fact]
        public void Can_query_using_in_on_enumerable()
        {

            using (var documentStore = GetDocumentStore())
            {
                var idsToLoad = new List<string> { "5", "7" };
                using (var session = documentStore.OpenSession())
                {
                    var testDocs = session.Query<TestDoc>()
                        .Where(x => x.Id.In(idsToLoad.Select(y => y))).ToList();
                }
            }
        }


        public class TestDoc
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

    }
}
