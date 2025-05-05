using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public class RavenDB_23649(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void TermIsNotLeakedToAnotherIndexEntryWhenUsingACustomAnalyzerViaLuceneAdapter(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            foreach (var dto in new List<Dto> { new() { Surname = "O'Cfirst" }, new() { Surname = "O'Second" }})
                session.Store(dto);
            
            session.SaveChanges();
        }

        var index = new Index();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<Dto>(
                    """
                    from index 'Index'
                              where exact(Surname == "O'Cfirst")
                    """)
                .ToList();
            WaitForUserToContinueTheTest(store);
            Assert.Equal(1, results.Count);
            Assert.Equal("O'Cfirst", results[0].Surname);
        }
    }
    
    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDynamicallyCreateExactAnalyzerForSearchWildcardQuery(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenSession())
        {
            foreach (var dto in new List<Dto> { new() { Surname = "O'Cfirst" }, new() { Surname = "O'Second" }, new() { Surname = "Third" }, })
                session.Store(dto);
            
            session.SaveChanges();
        }

        var index = new Index();
        index.Execute(store);
        Indexes.WaitForIndexing(store);

        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<Dto>(
                    """
                    from index 'Index'
                              where boost(search(Surname, $term), 10)
                              order by score()
                    """)
                .AddParameter("term", "O'C*")
                .ToList();
            Assert.Equal(1, results.Count);
            Assert.Equal("O'Cfirst", results[0].Surname);
            
            results = session.Advanced.RawQuery<Dto>(
                    """
                    from index 'Index'
                              where search(SurnameAnalyzed, $term)
                              order by score()
                    """)
                .AddParameter("term", "O'C*")
                .ToList();
            Assert.Equal(2, results.Count);
            Assert.Equal("O'Cfirst", results[0].Surname);
            Assert.Equal("O'Second", results[1].Surname);
            
            results = session.Advanced.RawQuery<Dto>(
                    """
                    from index 'Index'
                              where boost(search(Surname, $term), 10) or search(SurnameAnalyzed, $term)
                              order by score()
                    """)
                .AddParameter("term", "O'C*")
                .ToList();
            Assert.Equal(2, results.Count);
            Assert.Equal("O'Cfirst", results[0].Surname);
            Assert.Equal("O'Second", results[1].Surname);
        }
    }
    
    private class Dto
    {
        public string Surname { get; set; }
    }

    private class Index : AbstractIndexCreationTask
    {
        public override string IndexName => "Index";

        public override IndexDefinition CreateIndexDefinition()
        {
            return new IndexDefinition
            {
                Maps =
                {
                    """
                    docs.Dtos.Select(dto => new {
                        Surname = dto.Surname,
                        SurnameAnalyzed = dto.Surname,
                    })
                    """
                },
                Fields =
                {
                    { "SurnameAnalyzed", new IndexFieldOptions { Indexing = FieldIndexing.Search, Analyzer = "SimpleAnalyzer" } },
                    { "Surname", new IndexFieldOptions { Indexing = FieldIndexing.Search, Analyzer = "KeywordAnalyzer" } }
                }
            };
        }
    }
}
