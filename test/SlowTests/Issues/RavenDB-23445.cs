using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23445 : RavenTestBase
{
    public RavenDB_23445(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void TestIndexingOfNulls(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var territory1 = new Territory() { Name = "New York" };
                var territory2 = new Territory() { Name = null };
                
                var dto1 = new Dto() { Territories = new List<Territory>() { territory1, territory2 } };
                var dto2 = new Dto() { Territories = new List<Territory>() { territory2, null } };
                
                session.Store(dto1);
                session.Store(dto2);
                
                session.SaveChanges();
                
                var results = session.Advanced.RawQuery<Dto>("from \"Dtos\" where vector.search(embedding.text(Territories[].Name), \"New York\")").ToList();
                
                Indexes.WaitForIndexing(store);
                
                Assert.Equal(1, results.Count);
            }
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineData(VectorEmbeddingType.Binary, 0.65f)]
    [InlineData(VectorEmbeddingType.Int8, 0.80f)]
    [InlineData(VectorEmbeddingType.Single, 0.80f)]
    public void CanCreateVectorIndexFromCSharp(VectorEmbeddingType vectorEmbeddingType, float similarity)
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Document() { Text = null, Text2 = string.Empty, TextArr = ["Cat has brown eyes.", "Car has brown eyes."] });
                session.Store(new Document() { Text2 = "Cat has brown eyes.", TextArr = ["Cat has brown eyes.", "Car has brown eyes."] });
                
                session.SaveChanges();
                
                new TextVectorIndex(vectorEmbeddingType).Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var res = session.Query<Document, TextVectorIndex>().VectorSearch(x => x.WithField(f => f.Vector), f => f.ByText("animal color"), similarity);
                
                var results = res.ToList();

                Assert.Equal(1, results.Count);
                Assert.Contains("Cat", results[0].Text2);
            }
        }
    }

    private class TextVectorIndex : AbstractIndexCreationTask<Document>
    {
        public TextVectorIndex()
        {
            //querying
        }

        public TextVectorIndex(VectorEmbeddingType vectorEmbeddingType)
        {
            Map = docs => from doc in docs
                          select new { Id = doc.Id, Vector = CreateVector(new List<string> { doc.Text, doc.Text2 }) };
            
            VectorIndexes.Add(x => x.Vector,
                new VectorOptions()
                {
                    SourceEmbeddingType = VectorEmbeddingType.Text,
                    DestinationEmbeddingType = vectorEmbeddingType
                });
        }
    }

    private class Document
    {
        public string Id { get; set; }
        public string Text { get; set; }
        public string Text2 { get; set; }
        public string[] TextArr { get; set; }
        public float[] Vector { get; set; }
    }

    private class Dto
    {
        public List<Territory> Territories { get; set; }
    }

    private class Territory
    {
        public string Name { get; set; }
    }
}
