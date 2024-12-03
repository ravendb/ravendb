using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_23245(ITestOutputHelper output) : RavenTestBase(output)
{
    private static string OrderByClause(bool isDescending) => isDescending ? "desc" : "asc";
    private static int CompareResult(bool isDescending) => isDescending ? -1 : 1; // note reverted arguments

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderByScoreScoreExposed(bool isDescending)
    {
        using var store = GetDatabaseWithDocuments();
        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<QueryResult>($"from index 'Index' v where v.Text == 'text' order by score() {OrderByClause(isDescending)} select {{ Score: v['@metadata']['@index-score'] , Numerical: v.Numerical}}").ToList();
            
            Assert.Equal(2, results.Count);
            Assert.Equal(CompareResult(isDescending), results[0].Numerical.CompareTo(results[1].Numerical));
            Assert.Equal(CompareResult(isDescending), results[0].Score.CompareTo(results[1].Score));
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderByNumScoreExposed(bool isDescending)
    {
        using var store = GetDatabaseWithDocuments();
        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<QueryResult>("from index 'Index' v where boost(v.Text == 'text', 10)" +
                                                                 $" order by v.EqualNumberForTestSecondParameter as long asc, score() {OrderByClause(isDescending)} select {{ Score: v['@metadata']['@index-score'] , Numerical: v.Numerical}}")
                .ToList();
            
            Assert.Equal(2, results.Count);
            Assert.Equal(CompareResult(isDescending), results[0].Score.CompareTo(results[1].Score));
            Assert.Equal(CompareResult(isDescending), results[0].Numerical.CompareTo(results[1].Numerical));
        }
    }
    
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [InlineData(true)]
    [InlineData(false)]
    public void OrderByStringScoreExposed(bool isDescending)
    {
        using var store = GetDatabaseWithDocuments();
        using (var session = store.OpenSession())
        {
            var results = session.Advanced.RawQuery<QueryResult>("from index 'Index' v where boost(v.TextNormal == 'text text', 10)" +
                                                                 " order by v.TextNormal, " +
                                                                 $"score() {OrderByClause(isDescending)} " +
                                                                 "select { Score: v['@metadata']['@index-score'] , Numerical: v.Numerical}")
                .ToList();
            
            Assert.Equal(2, results.Count);
            Assert.Equal(CompareResult(isDescending), results[0].Score.CompareTo(results[1].Score));
            Assert.Equal(CompareResult(isDescending), results[0].Numerical.CompareTo(results[1].Numerical));
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
            session.Store(new Dto(10, "text text", 1, 1));
            session.Store(new Dto(20, "text text", 2, 1));
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        return store;
    }

    private record Dto(float DocBoost, string Text, int Numerical, int EqualNumberForTestSecondParameter);

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
                select new { doc.Text, doc.Numerical, TextNormal = doc.Text, doc.EqualNumberForTestSecondParameter }.Boost(doc.DocBoost);
            Index(x => x.Text, FieldIndexing.Search);
        }
    }
}
