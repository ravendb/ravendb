using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_18096 : RavenTestBase
    {
        public RavenDB_18096(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Can_Parse_Short_In_Index()
        {
            using (var store = GetDocumentStore())
            {
                await new DocumentIndex().ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        Short = 0
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var docs = await session.Query<DocumentIndex.Result, DocumentIndex>()
                        .OfType<Document>()
                        .ToListAsync();

                    Assert.Equal(1, docs.Count);
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }

            public short Short { get; set; }
        }

        private class DocumentIndex : AbstractIndexCreationTask<Document>
        {
            public class Result
            {
                public short? Short { get; set; }
            }

            public DocumentIndex()
            {
                Map = documents => from document in documents
                    select new Result
                    {
                        Short = document.Id != null ? (short?)document.Short : null
                    };
            }
        }
    }
}
