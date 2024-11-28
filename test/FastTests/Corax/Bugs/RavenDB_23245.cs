using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_23245 : RavenTestBase
{
    public RavenDB_23245(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrderByScoreScoreExposed()
    {
        using var store = GetDatabaseWithDocuments();
        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<QueryResult>("from index 'Index' v where v.Text == 'text' select { Score: v['@metadata']['@index-score'] , Numerical: v.Numerical}").ToList();
            
            Assert.Equal(2, results.Count);
            Assert.True(results[0].Numerical > results[1].Numerical);
            Assert.True(results[0].Score > results[1].Score);
        }
    }
    
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrderByNumScoreExposed()
    {
        using var store = GetDatabaseWithDocuments();
        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<QueryResult>("from index 'Index' v where boost(v.Text == 'text', 10)" +
                                                                 " order by v.Numerical as long asc, score() select { Score: v['@metadata']['@index-score'] , Numerical: v.Numerical}")
                .ToList();
            
            Assert.Equal(2, results.Count);
            Assert.True(results[0].Numerical < results[1].Numerical);
            Assert.True(results[0].Score < results[1].Score);
        }
    }
    
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void OrderByStringScoreExposed()
    {
        using var store = GetDatabaseWithDocuments();
        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<QueryResult>("from index 'Index' v where boost(v.TextNormal == 'text text', 10)" +
                                                                 " order by v.TextNormal, score() select { Score: v['@metadata']['@index-score'] , Numerical: v.Numerical}")
                .ToList();
            
            Assert.Equal(2, results.Count);
            Assert.True(results[0].Score > results[1].Score);
        }
    }

    private IDocumentStore GetDatabaseWithDocuments()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxIncludeDocumentScore)] = true.ToString();
        };

        var store = GetDocumentStore(options);
        new Index().Execute(store);
        using (var session = store.OpenSession())
        {
            session.Store(new Dto(10, "text text", 1));
            session.Store(new Dto(20, "text text", 2));
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        return store;
    }

    private record Dto(float DocBoost, string Text, int Numerical);

    private class QueryResult
    {
        public int Numerical { get; set; }
        public float Score { get; set; }
    }
    
    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = dtos => from doc in dtos
                select new { doc.Text, doc.Numerical, TextNormal = doc.Text }.Boost(doc.DocBoost);
            Index(x => x.Text, FieldIndexing.Search);
        }
    }
}
