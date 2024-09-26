using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Raven.Client.Documents.Indexes.Analysis;
using Raven.Client.Documents.Operations.Analyzers;

namespace SlowTests.Issues;

public class RavenDB_22937 : RavenTestBase
{
    public RavenDB_22937(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void StaticIndexStandardAnalyzerCanSearchForWildcardsInSearch()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        new StandardIndex().Execute(store);
        IRavenQueryable<Dto> QueryFactory(IDocumentSession session) => session.Query<Dto, StandardIndex>().Customize(x => x.WaitForNonStaleResults());
        StandardAnalyzerCanSearchForWildcardsInSearch(store, QueryFactory, nameof(Dto.Name));
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void StaticIndexWithCustomSearchAnalyzerCanSearchForWildcardsInSearchWhenAnalyzerDoesntRemoveAsterisks()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition()
        {
            Name = nameof(FullTextSearchWithoutRemovingAsterisk), 
            Code = FullTextSearchWithoutRemovingAsterisk
        }));
        
        new FullTextSearchWithoutRemovingAsteriskIndex(nameof(FullTextSearchWithoutRemovingAsterisk)).Execute(store);
        IRavenQueryable<Dto> QueryFactory(IDocumentSession session) => session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>().Customize(x => x.WaitForNonStaleResults());
        StandardAnalyzerCanSearchForWildcardsInSearch(store, QueryFactory, nameof(Dto.Name));
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void AutoIndexStandardAnalyzerCanSearchForWildcardsInSearch()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        IRavenQueryable<Dto> QueryFactory(IDocumentSession session) => session.Query<Dto>().Customize(x => x.WaitForNonStaleResults());
        StandardAnalyzerCanSearchForWildcardsInSearch(store, QueryFactory, $"search({nameof(Dto.Name)})");
    }
    
    

    private void StandardAnalyzerCanSearchForWildcardsInSearch(IDocumentStore store, Func<IDocumentSession, IRavenQueryable<Dto>> createQuery, string indexFieldName)
    {
        using var session = store.OpenSession();
        session.Advanced.WaitForIndexesAfterSaveChanges();
        session.Store(new Dto("MacCOMMONiej"));
        session.Store(new Dto("RavCOMMONenDB"));
        session.SaveChanges();

        //startsWith:
        var result = createQuery(session)
            .ToDocumentQuery()
            .IncludeExplanations(out var explanations)
            .ToQueryable()
            .Search(x => x.Name, "Mac*")
            .First();
        Assert.Equal("MacCOMMONiej", result.Name);
        Assert.Contains($"{indexFieldName}:mac*", explanations.GetExplanations(result.Id)[0]);

        //endsWith
        result = createQuery(session)
            .ToDocumentQuery()
            .IncludeExplanations(out explanations)
            .ToQueryable()
            .Search(x => x.Name, "*db")
            .First();
        Assert.Equal("RavCOMMONenDB", result.Name);
        Assert.Contains($"{indexFieldName}:*db", explanations.GetExplanations(result.Id)[0]);

        //contains
        var results = createQuery(session)
            .ToDocumentQuery()
            .IncludeExplanations(out explanations)
            .ToQueryable()
            .Search(x => x.Name, "*COMMON*")
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains($"{indexFieldName}:*common*", explanations.GetExplanations(results[0].Id)[0]);
        Assert.Contains($"{indexFieldName}:*common*", explanations.GetExplanations(results[1].Id)[0]);
    }


    [RavenFact(RavenTestCategory.Querying)]
    public void CustomStandardAnalyzerSearchWillNotRestoreAsterisks()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition()
        {
            Name = nameof(StandardAnalyzerWrapperAnalyzer), 
            Code = StandardAnalyzerWrapperAnalyzer
        }));
        
        new FullTextSearchWithoutRemovingAsteriskIndex(nameof(StandardAnalyzerWrapperAnalyzer)).Execute(store);
        
        using var session = store.OpenSession();
        session.Advanced.WaitForIndexesAfterSaveChanges();
        session.Store(new Dto("Maciej"));
        session.Store(new Dto("RavenDB"));
        session.SaveChanges();

        //startsWith:
        var result = session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
            .ToDocumentQuery()
            .IncludeExplanations(out var explanations)
            .ToQueryable()
            .Search(x => x.Name, "maciej*")
            .First();
        Assert.Equal("Maciej", result.Name);
        Assert.Contains($"Name:maciej", explanations.GetExplanations(result.Id)[0]);

        //endsWith
        result = session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
            .ToDocumentQuery()
            .IncludeExplanations(out explanations)
            .ToQueryable()
            .Search(x => x.Name, "*ravendb")
            .First();
        Assert.Equal("RavenDB", result.Name);
        Assert.Contains($"Name:ravendb", explanations.GetExplanations(result.Id)[0]);

        //contains
        var results = session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
            .ToDocumentQuery()
            .IncludeExplanations(out explanations)
            .ToQueryable()
            .Search(x => x.Name, "*maciej*")
            .ToList();

        Assert.Equal(1, results.Count);
        Assert.Contains($"Name:maciej", explanations.GetExplanations(results[0].Id)[0]);
    }
    
    private class StandardIndex : AbstractIndexCreationTask<Dto>
    {
        public StandardIndex()
        {
            Map = dtos => dtos.Select(x => new { x.Id, x.Name });
            Index(x => x.Name, FieldIndexing.Search);
        }
    }

    private class FullTextSearchWithoutRemovingAsteriskIndex : AbstractIndexCreationTask<Dto>
    {
        public FullTextSearchWithoutRemovingAsteriskIndex()
        {
            // querying placeholder
        }
        
        public FullTextSearchWithoutRemovingAsteriskIndex(string customIndexName)
        {
            Map = dtos => from dto in dtos select new { dto.Id, dto.Name };
            Analyze(x => x.Name, customIndexName);
        }
    }

    private record Dto(string Name, string Id = null);

    private const string FullTextSearchWithoutRemovingAsterisk = @"
using System.IO;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Standard;
namespace CustomAnalyzers 
{

    public class FullTextSearchWithoutRemovingAsterisk : Analyzer
    {
        public FullTextSearchWithoutRemovingAsterisk() : base()
        {
        }

        public override TokenStream TokenStream(string fieldName, System.IO.TextReader reader)
        {
            var whitespaceTokenizer = new WhitespaceTokenizer(reader);
            var lowerCaseFilter = new LowerCaseFilter(whitespaceTokenizer);
            return lowerCaseFilter;
        }
    }
}
";
    
    public const string StandardAnalyzerWrapperAnalyzer =
        @"
using System.IO;
using Lucene.Net.Analysis; 
using Lucene.Net.Analysis.Standard;
namespace CustomAnalyzers
{
    public class StandardAnalyzerWrapperAnalyzer : StandardAnalyzer
    {
        public StandardAnalyzerWrapperAnalyzer() : base(Lucene.Net.Util.Version.LUCENE_30)
        {
        }

        public override TokenStream TokenStream(string fieldName, System.IO.TextReader reader)
        {
            return base.TokenStream(fieldName, reader);
        }
    }
}";
    
}
