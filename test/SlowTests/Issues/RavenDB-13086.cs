using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13086 : RavenTestBase
    {
        public RavenDB_13086(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ResultCacheShouldConsiderDocumentsLoadedInProjection(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                Document mainDocument;
                Document2 referencedDocument;

                using (var session = store.OpenSession())
                {
                    referencedDocument = new Document2
                    {
                        DataToUpdate = "original"
                    };
                    session.Store(referencedDocument);

                    mainDocument = new Document
                    {
                        References = new[]
                        {
                            new DocumentReference
                            {
                                Document2Id = referencedDocument.Id
                            },
                        }
                    };
                    session.Store(mainDocument);
                    session.SaveChanges();
                }

                string[] ProjectValuesLinq()
                {
                    using (var session = store.OpenSession())
                    {
                        var single = (from doc in session.Query<Document>().Where(x => x.Id == mainDocument.Id)
                                let referenced = RavenQuery.Load<Document2>(doc.References.Select(x => x.Document2Id))
                                select new Result
                                {
                                    Data = referenced
                                        .Select(x => x.DataToUpdate)
                                        .ToArray()
                                })
                            .Single();

                        return single.Data;
                    }
                }

                string[] ProjectValuesRql1()
                {
                    var query = $"from Documents as doc where id() = '{mainDocument.Id}'" +
@"select {
    Data: doc.References.map(function(x){return load(x.Document2Id);}).map(function(x){return x.DataToUpdate;})
}";
                    return ProjectValuesRql(query);
                }

                string[] ProjectValuesRql2()
                {
                    var query = $"from Documents as doc where id() = '{mainDocument.Id}'" +
                                @"load doc.References[].Document2Id as r[]
select {
    Data: r.map(function(x){return x.DataToUpdate;})
}";
                    return ProjectValuesRql(query);
                }

                string[] ProjectValuesRql(string query)
                {
                    using (var session = store.OpenSession())
                    {
                        var single = session.Advanced.RawQuery<Result>(query).Single();
                        return single.Data;
                    }
                }

                Assert.Contains(ProjectValuesLinq(), x => x == "original");
                Assert.Contains(ProjectValuesRql1(), x => x == "original");
                Assert.Contains(ProjectValuesRql2(), x => x == "original");

                using (var session = store.OpenSession())
                {
                    var dbDoc = session.Load<Document2>(referencedDocument.Id);
                    dbDoc.DataToUpdate = "modified";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var dbDoc = session.Load<Document2>(referencedDocument.Id);
                    Assert.Equal("modified", dbDoc.DataToUpdate);
                }

                Assert.Contains(ProjectValuesLinq(), x => x == "modified");
                Assert.Contains(ProjectValuesRql1(), x => x == "modified");
                Assert.Contains(ProjectValuesRql2(), x => x == "modified");
            }
        }

        private class Document
        {
#pragma warning disable 649
            public string Id;
#pragma warning restore 649
            public DocumentReference[] References;
        }

        private class DocumentReference
        {
            public string Document2Id;
        }

        private class Document2
        {
#pragma warning disable 649
            public string Id;
#pragma warning restore 649
            public string DataToUpdate;
        }

        private class Result
        {
            public string[] Data { get; set; }
        }
    }
}
