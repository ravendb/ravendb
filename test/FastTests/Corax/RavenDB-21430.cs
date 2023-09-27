using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class RavenDB_21430 : RavenTestBase
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

                using (db.NotificationCenter.GetStored(out var actions))
                {
                    var alertPersisted = actions.ToList();
                    Assert.Equal(1, alertPersisted.Count);

                    var complexFieldAlert = Raven.Server.NotificationCenter.Notifications.AlertRaised.FromJson("", alertPersisted[0].Json);
                    Assert.Equal(AlertType.Indexing_CoraxComplexItem, complexFieldAlert.AlertType);
                    Assert.Contains("Complex field in Corax auto index", complexFieldAlert.Title);
                }
            }
        }
    }

    private sealed record ComplexField(Query.Order Order, Query.Order Field, string Id = null);
}
