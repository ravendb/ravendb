using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_28122 : RavenTestBase
{
    public RavenDB_28122(ITestOutputHelper output) : base(output)
    {
    }

    private const string IndexName = "Activities/ByFinder";

    [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void FacetOnFieldThatHasNoTerms_ReturnsEmptyBuckets(Options options)
    {
        using var store = GetStoreWithActivities(options);
        using var session = store.OpenSession();

        var all = session.Query<Activity>(IndexName)
            .AggregateBy(new Facet { FieldName = "CampaignCodes" })
            .Execute();
        Assert.Empty(all["CampaignCodes"].Values);

        var filtered = session.Query<Activity>(IndexName)
            .Where(x => x.DealerId == "dealers/1")
            .AggregateBy(new Facet { FieldName = "CampaignCodes" })
            .Execute();
        Assert.Empty(filtered["CampaignCodes"].Values);
    }

    [RavenTheory(RavenTestCategory.Facets | RavenTestCategory.Corax)]
    [RavenData("from index 'Activities/ByFinder' select facet(Budgets < 10, Budgets >= 10)", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData("from index 'Activities/ByFinder' where DealerId = 'dealers/1' select facet(Budgets < 10, Budgets >= 10)", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void RangeFacetOnFieldThatHasNoTerms_CountsNothing(Options options, string rql)
    {
        using var store = GetStoreWithActivities(options);
        using var session = store.OpenSession();

        var result = session.Advanced.RawQuery<Activity>(rql).ExecuteAggregation();
        Assert.Equal(new[] { 0, 0 }, result["Budgets"].Values.Select(x => x.Count));
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData("from index 'Activities/ByFinder' where Budgets = 5", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData("from index 'Activities/ByFinder' where Budgets = 5.5", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    [RavenData("from index 'Activities/ByFinder' where DealerId = 'dealers/1' and Budgets = 5", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
    public void NumericTermOnFieldThatHasNoTerms_MatchesNothing(Options options, string rql)
    {
        using var store = GetStoreWithActivities(options);
        using var session = store.OpenSession();

        Assert.Empty(session.Advanced.RawQuery<Activity>(rql).ToList());
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData("from index 'Activities/ByFinder' order by CampaignCodes", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from index 'Activities/ByFinder' order by Budgets as long", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    [RavenData("from index 'Activities/ByFinder' order by Budgets as double", SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void OrderByFieldThatHasNoTerms_ReturnsAllDocuments(Options options, string rql)
    {
        using var store = GetStoreWithActivities(options);
        using var session = store.OpenSession();

        Assert.Equal(10, session.Advanced.RawQuery<Activity>(rql).ToList().Count);
    }

    private IDocumentStore GetStoreWithActivities(Options options)
    {
        var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            for (var i = 0; i < 10; i++)
                session.Store(new Activity { DealerId = "dealers/1", Campaigns = new List<Campaign> { new Campaign { JourneyId = "journeys/1" } } });
            session.SaveChanges();
        }

        // the campaigns have neither CampaignCode nor Budget, so these fields get no term at all (not even null)
        store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
        {
            Name = IndexName,
            Maps = { "from a in docs.Activities select new { a.DealerId, CampaignCodes = a.Campaigns.Select(c => c.CampaignCode), Budgets = a.Campaigns.Select(c => c.Budget) }" }
        }));
        Indexes.WaitForIndexing(store);
        return store;
    }

    private class Activity
    {
        public string DealerId { get; set; }
        public List<Campaign> Campaigns { get; set; }
    }

    private class Campaign
    {
        public string JourneyId { get; set; }
    }
}
