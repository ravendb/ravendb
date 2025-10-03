using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21702 : RavenTestBase
{
    public RavenDB_21702(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public void TestHighlightingOnMultipleFieldsAndNullValues()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var d1 = new Dto() { Name = "Cool and super long name just so it works in Lucene", City = null, Description = "Some dummy text that's quite long so it works in Lucene" };
                var d2 = new Dto() { Name = null, City = null, Description = "Some dummy text that's quite long so it works in Lucene" };
                
                session.Store(d1);
                session.Store(d2);
                
                session.SaveChanges();

                var coraxIndex = new CoraxDummyIndex();
                var luceneIndex = new LuceneDummyIndex();
                
                coraxIndex.Execute(store);
                luceneIndex.Execute(store);
                
                Indexes.WaitForIndexing(store);
                
                var tagsToUse = new HighlightingOptions
                {
                    PreTags = new[] { "*" },
                    PostTags = new[] { "*" }
                };

                var coraxResults = session.Query<Dto>(coraxIndex.IndexName)
                    .Where(x => x.City == null)
                    .Search(x => x.Name, "Cool")
                    .Search(x => x.Description, "Some")
                    .Highlight(x => x.Name, 18, 3, tagsToUse, out var coraxNameHighlightings)
                    .Highlight(x => x.City, 18, 3, tagsToUse, out var coraxCityHighlightings)
                    .Highlight(x => x.Description, 18, 3, tagsToUse, out var coraxDescriptionHighlightings)
                    .ToList();

                var luceneResults = session.Query<Dto>(luceneIndex.IndexName)
                    .Where(x => x.City == null)
                    .Search(x => x.Name, "Cool")
                    .Search(x => x.Description, "Some")
                    .Highlight(x => x.Name, 18, 3, tagsToUse, out var luceneNameHighlightings)
                    .Highlight(x => x.City, 18, 3, tagsToUse, out var luceneCityHighlightings)
                    .Highlight(x => x.Description, 18, 3, tagsToUse, out var luceneDescriptionHighlightings)
                    .ToList();
                
                Assert.Equal(coraxNameHighlightings.ResultIndents.Count(), 1);
                Assert.Equal(coraxCityHighlightings.ResultIndents.Count(), 0);
                Assert.Equal(coraxDescriptionHighlightings.ResultIndents.Count(), 2);
                
                Assert.Equal(luceneNameHighlightings.ResultIndents.Count(), 1);
                Assert.Equal(luceneCityHighlightings.ResultIndents.Count(), 0);
                Assert.Equal(luceneDescriptionHighlightings.ResultIndents.Count(), 2);
                
                Assert.Equal(coraxResults.Select(x => x.Id), luceneResults.Select(x => x.Id));
                
                for (var i = 0; i < coraxResults.Count; i++)
                {
                    var result = coraxResults[i];
                    
                    var coraxNameFragments = coraxNameHighlightings.GetFragments(result.Id);
                    var luceneNameFragments = luceneNameHighlightings.GetFragments(result.Id);
                    
                    Assert.All(coraxNameFragments, coraxNameFragment => Assert.True(coraxNameFragment.StartsWith("*Cool* and super")));
                    Assert.All(luceneNameFragments, luceneNameFragment => Assert.True(luceneNameFragment.StartsWith("*Cool* and super")));
                    
                    var coraxCityFragments = coraxCityHighlightings.GetFragments(result.Id);
                    var luceneCityFragments = luceneCityHighlightings.GetFragments(result.Id);
                    
                    Assert.Empty(coraxCityFragments);
                    Assert.Empty(luceneCityFragments);

                    var coraxDescriptionFragments = coraxDescriptionHighlightings.GetFragments(result.Id);
                    var luceneDescriptionFragments = luceneDescriptionHighlightings.GetFragments(result.Id);
                    
                    Assert.All(coraxDescriptionFragments, coraxDescriptionFragment => Assert.True(coraxDescriptionFragment.StartsWith("*Some* dummy text")));
                    Assert.All(luceneDescriptionFragments, luceneDescriptionFragment => Assert.True(luceneDescriptionFragment.StartsWith("*Some* dummy text")));
                }
            }
        }
    }

    private class Dto
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string City { get; set; }
        public string Description { get; set; }
    }

    private class CoraxDummyIndex : AbstractIndexCreationTask<Dto>
    {
        public CoraxDummyIndex()
        {
            Map = dtos => from dto in dtos
                select new { dto.Name, dto.City, dto.Description };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
            
            Store(dto => dto.Name, FieldStorage.Yes);
            Index(dto => dto.Name, FieldIndexing.Search);
            TermVector(dto => dto.Name, FieldTermVector.WithPositionsAndOffsets);
            
            Store(dto => dto.Description, FieldStorage.Yes);
            Index(dto => dto.Description, FieldIndexing.Search);
            TermVector(dto => dto.Description, FieldTermVector.WithPositionsAndOffsets);
        }
    }
    
    private class LuceneDummyIndex : AbstractIndexCreationTask<Dto>
    {
        public LuceneDummyIndex()
        {
            Map = dtos => from dto in dtos
                select new { dto.Name, dto.City, dto.Description };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
            
            Store(dto => dto.Name, FieldStorage.Yes);
            Index(dto => dto.Name, FieldIndexing.Search);
            TermVector(dto => dto.Name, FieldTermVector.WithPositionsAndOffsets);
            
            Store(dto => dto.Description, FieldStorage.Yes);
            Index(dto => dto.Description, FieldIndexing.Search);
            TermVector(dto => dto.Description, FieldTermVector.WithPositionsAndOffsets);
        }
    }
}
