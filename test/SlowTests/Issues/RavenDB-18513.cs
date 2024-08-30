using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Issues;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_18513 : RavenTestBase
{
    public RavenDB_18513(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task RevertRevisionShouldntCreateOrphanedRevisions()
    {
        DateTime beforeStore = DateTime.UtcNow - TimeSpan.FromDays(1);

        using var store = GetDocumentStore();
        var configuration = new RevisionsConfiguration
        {
            Default = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 100
            }
        };
        await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

        var user1 = new User { Id = "Users/1-A", Name = "Shahar" };
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(user1);
            await session.SaveChangesAsync();

            for (int i = 1; i <= 10; i++)
            {
                (await session.LoadAsync<User>(user1.Id)).Name = $"Shahar{i}";
                await session.SaveChangesAsync();
            }

            var user1RevCount = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
            Assert.Equal(11, user1RevCount);
        }

        // delete configuration
        var empyConfiguration = new RevisionsConfiguration();
        await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: empyConfiguration);
        // revert revision
        var operation = await store.Maintenance.SendAsync(new RevisionsHelper.RevertRevisionsOperation(beforeStore, 60));
        await operation.WaitForCompletionAsync(TimeSpan.FromSeconds(15)).ConfigureAwait(false);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<User>(user1.Id);
            Assert.Null(doc); // user1 should be deleted

            var user1RevCount = await session.Advanced.Revisions.GetCountForAsync(user1.Id);
            Assert.Equal(user1RevCount, 12); // 11 revisions and 1 deleted revision

            var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(user1.Id);
            var lastRevisionFlags = revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags);
            string deleteRevisionsFlag = DocumentFlags.DeleteRevision.ToString();

            // deleted doc && has revisions && last revisions isn't 'DeleteRevision' => doc has orphaned revisions
            Assert.True(lastRevisionFlags.Contains(deleteRevisionsFlag));

            string revertedFlag = DocumentFlags.Reverted.ToString();
            Assert.True(lastRevisionFlags.Contains(revertedFlag));
        }
    }

    private class User
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }
}

