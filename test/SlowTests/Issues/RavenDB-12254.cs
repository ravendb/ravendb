using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12254 : RavenTestBase
    {
        private class QueryResult
        {
            public int Int1 { get; set; }
            public int Int2 { get; set; }
            public string Id { get; set; }
        }

        private class RavenDocument
        {
            public int Int { get; set; }
            public string Id { get; set; }
        }

        [Fact]
        public void CanUseLetClauseWithConditionalLoad()
        {
            using (var store = GetDocumentStore())
            {
                var inputDocumentId = "ravenDocuments/1";
                var externalDocumentId = "ravenDocuments/100";

                using (var session = store.OpenSession())
                {
                    session.Store(new RavenDocument(), inputDocumentId);
                    session.Store(new RavenDocument
                    {
                        Int = 100
                    }, externalDocumentId);

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var inputDocs = session.Query<RavenDocument>()
                        .Where(doc => doc.Id == inputDocumentId);

                    var query = from d in inputDocs
                                let externalDocument = d.Id == null ? null : RavenQuery.Load<RavenDocument>(externalDocumentId)
                                let x = d.Id == null ? 0 : 10
                                select new QueryResult
                                {
                                    Id = d.Id,
                                    Int1 = x,
                                    Int2 = externalDocument.Int + 1
                                };

                    RavenTestHelper.AssertEqualRespectingNewLines(
@"declare function output(d, $p1) {
	var externalDocument = id(d)==null?null:load($p1);
	var x = id(d)==null?0:10;
	return { Id : id(d), Int1 : x, Int2 : externalDocument.Int+1 };
}
from RavenDocuments as d where id() = $p0 select output(d, $p1)"
    , query.ToString());

                    var result = query.First();

                    Assert.Equal(inputDocumentId, result.Id);
                    Assert.Equal(10, result.Int1);
                    Assert.Equal(101, result.Int2);

                }
            }
        }
    }
}
