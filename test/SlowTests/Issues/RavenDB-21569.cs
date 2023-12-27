using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21569 : RavenTestBase
{
    public RavenDB_21569(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task Delete_Outdated_Revisions_By_EnforceConfig_When_Revisions_Isnt_OrderedBy_LastModified()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var configuration = new RevisionsConfiguration
        {
            Collections = new Dictionary<string, RevisionsCollectionConfiguration>()
            {
                ["Users"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                }
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        await CreateRevisions(store, database);


        configuration = new RevisionsConfiguration
        {
            Collections = new Dictionary<string, RevisionsCollectionConfiguration>()
            {
                ["Users"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionAgeToKeep = TimeSpan.FromDays(365),
                    PurgeOnDelete = true
                }
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        using (var token = new OperationCancelToken(database.Configuration.Databases.OperationTimeout.AsTimeSpan, database.DatabaseShutdown, CancellationToken.None))
            await database.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, true, token: token);

        using (var session = store.OpenAsyncSession())
        {
            var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
            Assert.Equal(10, doc1RevCount);
        }
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task Delete_Outdated_Revisions_By_EnforceConfig_WithMaxUponUpdate_When_Revisions_Isnt_OrderedBy_LastModified()
    {
        using var store = GetDocumentStore();
        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        var configuration = new RevisionsConfiguration
        {
            Collections = new Dictionary<string, RevisionsCollectionConfiguration>()
            {
                ["Users"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                }
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        await CreateRevisions(store, database);


        configuration = new RevisionsConfiguration
        {
            Collections = new Dictionary<string, RevisionsCollectionConfiguration>()
            {
                ["Users"] = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionAgeToKeep = TimeSpan.FromDays(365),
                    MaximumRevisionsToDeleteUponDocumentUpdate = 3,
                    PurgeOnDelete = true
                }
            }
        };
        await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

        using (var token = new OperationCancelToken(database.Configuration.Databases.OperationTimeout.AsTimeSpan, database.DatabaseShutdown, CancellationToken.None))
            await database.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, true, token: token);

        using (var session = store.OpenAsyncSession())
        {
            var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
            Assert.Equal(10, doc1RevCount); // same but happens in batches
        }
    }

    private async Task CreateRevisions(DocumentStore store, DocumentDatabase database)
    {
        for (int i = 1; i <= 5; i++)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = $"New{i}" }, "Docs/1");
                await session.SaveChangesAsync();
            }
        }

        database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-400);
        for (int i = 1; i <= 5; i++)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = $"Old{i}" }, "Docs/1");
                await session.SaveChangesAsync();
            }
        }
        database.Time.UtcDateTime = () => DateTime.UtcNow;

        for (int i = 6; i <= 10; i++)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = $"New{i}" }, "Docs/1");
                await session.SaveChangesAsync();
            }
        }
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}

