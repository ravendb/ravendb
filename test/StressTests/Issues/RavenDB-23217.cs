using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using FastTests.Utils;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;


namespace StressTests.Issues
{
    internal class RavenDB_23217 : ReplicationTestBase
    {
        public RavenDB_23217(ITestOutputHelper output) : base(output)
        {
        }

        public async Task Shuould_Include_One_Revision_When_Using_RevisionBefore_Only_On_GetDocumentsCommand()
        {
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

            // Create a doc with 4 revisions
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New1" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New2" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            string[] revisionsChangeVectors = null;

            using (var session = store.OpenAsyncSession())
            {
                revisionsChangeVectors = (await session.Advanced.Revisions.GetMetadataForAsync("Docs/1")).Select(metadata =>
                {
                    metadata.TryGetValue(Constants.Documents.Metadata.ChangeVector, out string cv);
                    return cv;
                }).ToArray();
            }

            var command = new GetDocumentsCommand(
                ids: new[] { "Docs/1" },
                includes: null,
                counterIncludes: null,
                // Specify the change-vectors of the revisions to include
                null,
                revisionIncludeByDateTimeBefore: DateTime.Now + TimeSpan.FromDays(15),
                timeSeriesIncludes: null,
                compareExchangeValueIncludes: null,
                metadataOnly: false);

            using (var requestExecutor = store.GetRequestExecutor())
            using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
            {
                await requestExecutor.ExecuteAsync(command, ctx);

                Assert.Equal(1, command.Result.RevisionIncludes.Length); // Fail - it is 2 - same revision is shown twice
            }

        }
        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
