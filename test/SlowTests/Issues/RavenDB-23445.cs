using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23445 : RavenTestBase
{
    public RavenDB_23445(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Vector)]
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

    [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Vector)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void TestIndexingOfNullsInNumericalData(Options options)
    {
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };
        
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var d1 = new Document() { Id = "docs/1", Vectors = [ null, [0.1f, 0.2f, 0.3f, 0.4f], null ] };
                var d2 = new Document() { Id = "docs/2", Vectors = [ null, [1.1f, 1.2f, 1.3f] ] };
                var d3 = new Document() { Id = "docs/3", Vectors = [ null, [-0.5f, -0.6f, -0.7f, -0.8f] ] };
                
                session.Store(d1);
                session.Store(d2);
                session.Store(d3);
                
                session.SaveChanges();
                
                var queriedEmbedding1 = new float[] { 0.1f, 0.2f, 0.3f, 0.4f };
                
                var res = session.Query<Document>().VectorSearch(x => x.WithEmbedding("Vectors"), factory => factory.ByEmbedding(queriedEmbedding1), minimumSimilarity: 0.9f).ToList();
                
                WaitForUserToContinueTheTest(store);
                
                Assert.Single(res);
                Assert.Equal("docs/1", res[0].Id);
                
                var queriedEmbedding2 = new float[] { -0.5f, -0.6f, -0.7f, -0.8f };
                
                res = session.Query<Document>().VectorSearch(x => x.WithEmbedding("Vectors"), factory => factory.ByEmbedding(queriedEmbedding2), minimumSimilarity: 0.9f).ToList();

                Assert.Single(res);
                Assert.Equal("docs/3", res[0].Id);
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
        public float?[][] Vectors { get; set; }
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
