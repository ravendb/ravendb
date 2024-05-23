using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Corax;

public sealed class RavenDB_21430 : RavenTestBase
{
    public RavenDB_21430(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public async Task CanIndexComplexFieldInAutoIndex()
    {
        using (var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax)))
        {
            var db = await GetDatabase(store.Database);
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new ComplexField(new Query.Order() {Company = "Maciej"}, new Query.Order() {Company = "Test"}));
                await session.StoreAsync(new ComplexField(null, null));
                await session.SaveChangesAsync();

                var result = await session.Query<ComplexField>()
                    .Customize(i => i.WaitForNonStaleResults())
                    .Statistics(out var stats).Where(x => x.Order != null && x.Field != null)
                    .ToListAsync();
                Assert.False(stats.IsStale);

                Assert.Equal(1, result.Count);
                var indexErrors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] {stats.IndexName}));
                Assert.Empty(indexErrors[0].Errors);

                WaitForValue(() =>
                {
                    using var _ = db.NotificationCenter.GetStored(out var actions);
                    var persistedAlerts = actions.ToList();
                    bool foundCoraxHint = false;

                    foreach (var alert in persistedAlerts)
                    {
                        try
                        {
                            var complexFieldAlert = AlertRaised.FromJson("", persistedAlerts[0].Json);
                            Assert.Equal(AlertType.Indexing_CoraxComplexItem, complexFieldAlert.AlertType);
                            Assert.Contains("Complex field in Corax auto index", complexFieldAlert.Title);
                            foundCoraxHint = true;
                            break;
                        }
                        catch
                        {
                            //skip others
                        }
                    }
                    
                    
                    return  foundCoraxHint;
                }, true);
            }
        }
    }

    [RavenFact(RavenTestCategory.Querying | RavenTestCategory.Indexes | RavenTestCategory.Corax)]
    public async Task SearchComplexField()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(new ComplexField(new Query.Order() {Company = "Maciej"}, new Query.Order() {Company = "Test"}));
        await session.StoreAsync(new ComplexField(null, null));
        await session.SaveChangesAsync();
        
        var index = new Index();
        await index.ExecuteAsync(store);

        var errors = await WaitForPredicateAsync(x => x.Length > 0 && x[0].Errors.Length > 0,  () => store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { index.IndexName })));
        Assert.Equal(index.IndexName, errors[0].Name);
        Assert.Contains("he value of 'Order' field is a complex object", errors[0].Errors[0].Error);
        var result = await session.Query<ComplexField, Index>()
            .Search(x => x.Order, "maciej")
            .ToListAsync();
        Assert.Equal(0, result.Count);
    }
    
    private sealed class Index : AbstractIndexCreationTask<ComplexField>
    {
        public Index()
        {
            Map = fields => fields.Select(i => new { i.Order });
            Index(x => x.Order, FieldIndexing.Search);
        }
    }

    private sealed class ComplexField(Query.Order Order, Query.Order Field, string Id = null)
    {
        public Query.Order Order { get; init; } = Order;
        public Query.Order Field { get; init; } = Field;
        public string Id { get; init; } = Id;

        public void Deconstruct(out Query.Order Order, out Query.Order Field, out string Id)
        {
            Order = this.Order;
            Field = this.Field;
            Id = this.Id;
        }
    }
}
