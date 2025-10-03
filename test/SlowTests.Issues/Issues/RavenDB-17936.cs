using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17936 : RavenTestBase
{
    public RavenDB_17936(ITestOutputHelper output) : base(output)
    {
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void IndexDictionaryValuesTest()
    {
        using var store = GetDocumentStore();

        new DocIndex().Execute(store);

        using (var session = store.OpenSession())
        {
            session.Store(new Doc
            {
                Id = "doc-1",
                SubDocs = new Dictionary<string, SubDoc>
                {
                    { "subDoc-1A", new SubDoc { SubDocId = "subDoc-1A", MultiStrValues = new[] { "val1A1", "val1A2" } } },
                    { "subDoc-1B", new SubDoc { SubDocId = "subDoc-1B", MultiStrValues = new[] { "val1B1", "val1B2" } } },
                }
            });
            
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var matchUsingSelect = session.Query<DocView, DocIndex>()
                .Where(x => x.AllMultiStrValues.Contains("val1A1"))
                .ToArray();

            Assert.Single(matchUsingSelect);
            Assert.Equal("doc-1", matchUsingSelect[0].Id);
            
            var matchUsingValuesSelectMany = session.Query<DocView, DocIndex>()
                .Where(x => x.AllMultiStrValuesUsingValues.Contains("val1A1"))
                .ToArray();

            Assert.Single(matchUsingValuesSelectMany);
            Assert.Equal("doc-1", matchUsingValuesSelectMany[0].Id);
        }
    }

    private class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs =>
                from doc in docs
                select new
                {
                    doc.Id,
                    AllMultiStrValuesUsingValues = doc.SubDocs.Values.SelectMany(x => x.MultiStrValues).ToList(),
                    AllMultiStrValues = doc.SubDocs.SelectMany(x => x.Value.MultiStrValues).ToList()
                };

            StoreAllFields(FieldStorage.Yes);
        }
    }
    
    private class Doc
    {
        public string Id { get; set; }
        public Dictionary<string, SubDoc> SubDocs { get; set; }
    }

    private class SubDoc
    {
        public string SubDocId { get; set; }
        public string[] MultiStrValues { get; set; }
    }

    private class DocView
    {
        public string Id { get; set; }
        public string[] AllMultiStrValues { get; set; }
        public string[] AllMultiStrValuesUsingValues { get; set; }
    }
    
    [RavenFact(RavenTestCategory.Indexes)]
    public void CheckNestedDictionaries()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var d1 = new Dto() 
                { 
                    SubDtos = new Dictionary<string, SubDto>
                    {
                        { "subDoc-1A", new SubDto { MultiStrValues = new Dictionary<string, string> { {"val1A1", "val1A2" } } } },
                        { "subDoc-1B", new SubDto { MultiStrValues = new Dictionary<string, string> { {"val1B1", "val1B2" } } } }
                    }
                };
                
                session.Store(d1);
                
                session.SaveChanges();

                var index = new DummyIndex();
                
                index.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var res = session.Query<DummyIndex.IndexResult>(index.IndexName).ProjectInto<DummyIndex.IndexResult>().ToList();
                
                Assert.Equal(1, res.Count);
                Assert.NotNull(res[0]);
            }
        }
    }

    private class DummyIndex : AbstractIndexCreationTask<Dto>
    {
        public class IndexResult
        {
            public object AllMultiStrValuesUsingValues { get; set; }
            public object AllMultiStrValues { get; set; }
        }
        public DummyIndex()
        {
            Map = dtos => from dto in dtos
                select new IndexResult
                {
                    AllMultiStrValuesUsingValues = dto.SubDtos.Values.SelectMany(x => x.MultiStrValues),
                    AllMultiStrValues = dto.SubDtos.SelectMany(x => x.Value.MultiStrValues)
                };
            
            StoreAllFields(FieldStorage.Yes);
        }
    }
    
    private class Dto
    {
        public Dictionary<string, SubDto> SubDtos { get; set; }
    }

    private class SubDto
    {
        public Dictionary<string, string> MultiStrValues { get; set; }
    }
}
