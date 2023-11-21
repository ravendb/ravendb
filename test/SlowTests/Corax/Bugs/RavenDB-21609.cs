using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax.Bugs;

public class RavenDB_21609 : RavenTestBase
{
    public RavenDB_21609(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [InlineData(218827642)] // 7228 docs
    [InlineData(1635325874)] // 12187 docs
    [InlineData(650027346)] // 12256 docs
    [InlineData(2116002769)] // 15834 docs
    public void SortingWillReturnExactlySameResultsAndQueryWithoutSortingMatch(int seed)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        var random = new Random(seed);

        var howManyTerms = random.Next(3, 20);
        var terms = Enumerable.Range(0, howManyTerms).Select(x => "SecretName" + string.Join("", Enumerable.Range(0, x).Select(x => "a"))).ToArray();
        var howManyDocument = random.Next(5000, 100_000);
        List<Dto> db = new();       
        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < howManyDocument; ++i)
            {
                var type = i % 11 == 0 ? "Best" : "Bad";
                var version = i % 22 == 0 ? "123" : "321";
                var name = terms[random.Next(0, terms.Length)];
                var entity = new Dto(name, type, version, true, DateTime.Now.AddDays(i));
                bulk.Store(entity);
                db.Add(entity);
            }
        }

        
        new DtoIndex(SearchEngineType.Corax).Execute(store);
        new DtoIndex(SearchEngineType.Lucene).Execute(store);
        Indexes.WaitForIndexing(store);
        
        using var session = store.OpenSession();
        var query = session.Query<Dto>("Corax/DtoIndex")
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name.StartsWith("SecretName") &&
                        x.Version.StartsWith("123") &&
                        x.Type == "Best")
            .OrderByDescending(x => x.Time).ToList();
        
        var luceneQuery = session.Query<Dto>("Lucene/DtoIndex")
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name.StartsWith("SecretName") &&
                        x.Version.StartsWith("123") &&
                        x.Type == "Best")
            .OrderByDescending(x => x.Time).ToList();
        
        var queryWithoutSorting = session.Query<Dto, DtoIndex>()
            .Customize(x => x.WaitForNonStaleResults())
            .Where(x => x.Name.StartsWith("SecretName") &&
                        x.Version.StartsWith("123") &&
                        x.Type == "Best")
            .ToList();
        var linq = db.Where(x => x.Name.StartsWith("SecretName") &&
                                 x.Version.StartsWith("123") &&
                                 x.Type == "Best").ToList();
        
        Assert.Equal(linq.Count, queryWithoutSorting.Count);
        Assert.Equal(luceneQuery.Count, queryWithoutSorting.Count);
        Assert.Equal(queryWithoutSorting.Count, query.Count);
    }

    private record Dto(string Name, string Type, string Version, bool Boolean, DateTime Time, string Id = null);

    private class DtoIndex : AbstractIndexCreationTask<Dto>
    {
        private string _indexName;
        
        public override string IndexName { get => _indexName; }

        public DtoIndex()
        {
        }
        
        public DtoIndex(SearchEngineType searchEngineType)
        {
            Map = dtos => dtos.Select(x => new {x.Name, x.Version, x.Time, x.Type});

            _indexName = (searchEngineType is Raven.Client.Documents.Indexes.SearchEngineType.Corax ? "Corax/" : "Lucene/") + "DtoIndex";
            SearchEngineType = searchEngineType;
        }
    }
}
