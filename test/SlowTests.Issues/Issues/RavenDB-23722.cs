using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_23722 : RavenTestBase
{
    public RavenDB_23722(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void RelevantExceptionOnEmbeddingsLengthMismatch(Options options)
    {
        var v1 = new[] { 0.1f, 0.2f, 0.3f };
        var v2 = new[] { 0.1f, 0.2f, 0.3f, 0.4f };
        
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var doc1 = new Dto();
                session.Store(doc1);
                session.SaveChanges();      

                using var memStream = new MemoryStream(MemoryMarshal.Cast<float, byte>(v1).ToArray());
                session.Advanced.Attachments.Store(doc1, "embedding", memStream);
                session.SaveChanges();

                var index = new CSharpIndexWithVector();
                index.Execute(store);
                Indexes.WaitForIndexing(store);
                var errors = Indexes.WaitForIndexingErrors(store, errorsShouldExists: false);
                Assert.Null(errors);
                
                var ex = Assert.Throws<RavenException>(() => session.Query<CSharpIndexWithVector.IndexEntry, CSharpIndexWithVector>()
                    .VectorSearch(f => f.WithField(s => s.Vector), v => v.ByEmbedding(v2))
                    .ProjectInto<Dto>()
                    .FirstOrDefault());

                Assert.Contains("Vector field `Vector` has 3 dimensions, but the vector passed to vector.search() has 4 dimensions", ex.Message);
            }
        }
    }
    
    private class CSharpIndexWithVector : AbstractIndexCreationTask<Dto>
    {
        public class IndexEntry
        {
            public object Vector { get; set; }
        }

        public CSharpIndexWithVector()
        {
            Map = documents => from document in documents
                let embeddings = LoadAttachment(document, "embedding")
                select new
                {
                    Vector = CreateVector(embeddings.GetContentAsStream())
                };
        }
    }

    private class Dto;
}
