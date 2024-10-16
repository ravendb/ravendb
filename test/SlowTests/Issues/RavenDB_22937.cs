using System;
using System.IO;
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
using Raven.Client.Documents.Operations.Backups;

namespace SlowTests.Issues;

public class RavenDB_22937 : RavenTestBase
{
    public RavenDB_22937(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenExplicitData(searchEngine: RavenSearchEngineMode.All)]
    public void StaticIndexStandardAnalyzerCanSearchForWildcardsInSearch(RavenTestParameters parameters)
    {
        using var store = GetDocumentStore(parameters.Options);
        new StandardIndex().Execute(store);
        IRavenQueryable<Dto> QueryFactory(IDocumentSession session) => session.Query<Dto, StandardIndex>().Customize(x => x.WaitForNonStaleResults());
        switch (parameters.SearchEngine)
        {
            case RavenSearchEngineMode.Lucene:
                StandardAnalyzerCanSearchForWildcardsInSearch(store, QueryFactory, nameof(Dto.Name));
                break;
            case RavenSearchEngineMode.Corax:
                StandardAnalyzerCanSearchForWildcardsInSearchCorax(store, QueryFactory, nameof(Dto.Name));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(parameters.SearchEngine), parameters.SearchEngine, null);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenExplicitData(searchEngine: RavenSearchEngineMode.All)]
    public void StaticIndexWithCustomSearchAnalyzerCanSearchForWildcardsInSearchWhenAnalyzerDoesntRemoveAsterisks(RavenTestParameters parameters)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        store.Maintenance.Send(new PutAnalyzersOperation(new AnalyzerDefinition()
        {
            Name = nameof(FullTextSearchWithoutRemovingAsterisk), 
            Code = FullTextSearchWithoutRemovingAsterisk
        }));
        
        new FullTextSearchWithoutRemovingAsteriskIndex(nameof(FullTextSearchWithoutRemovingAsterisk)).Execute(store);
        IRavenQueryable<Dto> QueryFactory(IDocumentSession session) => session
            .Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
            .Customize(x => x.WaitForNonStaleResults());

        switch (parameters.SearchEngine)
        {
            case RavenSearchEngineMode.Lucene:
                StandardAnalyzerCanSearchForWildcardsInSearch(store, QueryFactory, nameof(Dto.Name));
                break;
            case RavenSearchEngineMode.Corax:
                StandardAnalyzerCanSearchForWildcardsInSearchCorax(store, QueryFactory);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(parameters.SearchEngine), parameters.SearchEngine, null);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenExplicitData(searchEngine: RavenSearchEngineMode.All)]
    public void AutoIndexStandardAnalyzerCanSearchForWildcardsInSearch(RavenTestParameters parameters)
    {
        using var store = GetDocumentStore(parameters.Options);
        IRavenQueryable<Dto> QueryFactory(IDocumentSession session) => session.Query<Dto>().Customize(x => x.WaitForNonStaleResults());
        switch (parameters.SearchEngine)
        {
            case RavenSearchEngineMode.Lucene:
                StandardAnalyzerCanSearchForWildcardsInSearch(store, QueryFactory, $"search({nameof(Dto.Name)})");
                break;
            case RavenSearchEngineMode.Corax:
                StandardAnalyzerCanSearchForWildcardsInSearchCorax(store, QueryFactory, $"search({nameof(Dto.Name)})");
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(parameters.SearchEngine), parameters.SearchEngine, null);
        }
    }
    
    // No support for explanations inside Corax, so different assertions
    private void StandardAnalyzerCanSearchForWildcardsInSearchCorax(IDocumentStore store, Func<IDocumentSession, IRavenQueryable<Dto>> createQuery, string indexFieldName)
    {
        using var session = store.OpenSession();
        session.Advanced.WaitForIndexesAfterSaveChanges();
        session.Store(new Dto("MacCOMMONiej"));
        session.Store(new Dto("RavCOMMONenDB"));
        session.SaveChanges();

        //startsWith:
        var result = createQuery(session)
            .Search(x => x.Name, "Mac*")
            .First();
        Assert.Equal("MacCOMMONiej", result.Name);

        //endsWith
        result = createQuery(session)
            .Search(x => x.Name, "*db")
            .First();
        Assert.Equal("RavCOMMONenDB", result.Name);

        //contains
        var results = createQuery(session)
            .Search(x => x.Name, "*COMMON*")
            .ToList();

        Assert.Equal(2, results.Count);
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

    private void StandardAnalyzerCanSearchForWildcardsInSearchCorax(IDocumentStore store, Func<IDocumentSession, IRavenQueryable<Dto>> createQuery)
    {
        using var session = store.OpenSession();
        session.Advanced.WaitForIndexesAfterSaveChanges();
        session.Store(new Dto("MacCOMMONiej"));
        session.Store(new Dto("RavCOMMONenDB"));
        session.SaveChanges();

        //startsWith:
        var result = createQuery(session)
            .Search(x => x.Name, "Mac*")
            .First();
        Assert.Equal("MacCOMMONiej", result.Name);

        //endsWith
        result = createQuery(session)
            .Search(x => x.Name, "*db")
            .First();
        Assert.Equal("RavCOMMONenDB", result.Name);

        //contains
        var results = createQuery(session)
            .Search(x => x.Name, "*COMMON*")
            .ToList();

        Assert.Equal(2, results.Count);
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void CustomStandardAnalyzerSearchWillNotRestoreAsterisksLucene()
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
        Assert.Contains($"Name:maciej)", explanations.GetExplanations(results[0].Id)[0]);
    }
    
    [RavenFact(RavenTestCategory.Querying)]
    public void CustomStandardAnalyzerSearchWillNotRestoreAsterisksCorax()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
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
            .Search(x => x.Name, "mac*")
            .FirstOrDefault();
        Assert.Null(result);

        //endsWith
        result = session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
            .Search(x => x.Name, "*avendb")
            .FirstOrDefault();
        Assert.Null(result);


        //contains
        result = session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
            .Search(x => x.Name, "*ven*")
            .FirstOrDefault();
        Assert.Null(result);   
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    public void BackwardCompatibilityForWildcardQueries()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "RavenDB_22937.ravendb-snapshot");
        ExtractFile(fullBackupPath);
        using (var store = GetDocumentStore())
        {
            var databaseName = GetDatabaseName();

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration { BackupLocation = backupPath, DatabaseName = databaseName }))
            {
                using var session = store.OpenSession(databaseName);
                
                var result = session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
                    .Search(x => x.Name, "mac*")
                    .FirstOrDefault();
                Assert.NotNull(result);

                //endsWith
                result = session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
                    .Search(x => x.Name, "*avendb")
                    .FirstOrDefault();
                Assert.NotNull(result);


                //contains
                result = session.Query<Dto, FullTextSearchWithoutRemovingAsteriskIndex>()
                    .Search(x => x.Name, "*ven*")
                    .FirstOrDefault();
                Assert.NotNull(result);   
            }
        }

        void ExtractFile(string path)
        {
            using (var file = File.Create(path))
            using (var stream = typeof(RavenDB_22937).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22937.RavenDB_22937.ravendb-snapshot"))
            {
                stream.CopyTo(file);
            }
        }
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
