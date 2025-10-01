using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client;
using Raven.Client.Documents.Commands;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_23217 : ReplicationTestBase
    {
        public RavenDB_23217(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Shuould_Include_One_Revision_When_Using_RevisionBefore_Only_On_GetDocumentsCommand()
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 } };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

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

            var command = new GetDocumentsCommand(
                ids: new[] { "Docs/1" },
                includes: null,
                counterIncludes: null,
                revisionsIncludesByChangeVector: null,
                revisionIncludeByDateTimeBefore: DateTime.Now + TimeSpan.FromDays(15), // should include 1 revision (last before this datetime)
                timeSeriesIncludes: null,
                compareExchangeValueIncludes: null,
                metadataOnly: false,
                conventions: store.Conventions);
            
            using (var requestExecutor = store.GetRequestExecutor())
            using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
            {
                await requestExecutor.ExecuteAsync(command, ctx);
            
                Assert.Equal(1, command.Result.RevisionIncludes.Length); // Fail - it is 2 - same revision is shown twice
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task Shuouldnt_Stop_Including_Revisions_After_Encounter_Not_Existed_Revision()
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 } };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

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

                await session.StoreAsync(new User
                {
                    Name = revisionsChangeVectors[0],
                    Names = new [] { revisionsChangeVectors[0], "ABC", revisionsChangeVectors[1] }
                }, "Docs/1");
                await session.SaveChangesAsync();
            }

            var command = new GetDocumentsCommand(
                ids: new[] { "Docs/1" },
                includes: null,
                counterIncludes: null,
                // Specify the change-vectors of the revisions to include
                revisionsIncludesByChangeVector: new[] { "Name", "Names" },
                revisionIncludeByDateTimeBefore: null,
                timeSeriesIncludes: null,
                compareExchangeValueIncludes: null,
                metadataOnly: false,
                conventions: store.Conventions);

            using (var requestExecutor = store.GetRequestExecutor())
            using (requestExecutor.ContextPool.AllocateOperationContext(out var ctx))
            {
                await requestExecutor.ExecuteAsync(command, ctx);

                Assert.Equal(2, command.Result.RevisionIncludes.Length); // Fail - it is 1 - should include also revisionsChangeVectors[1], not only revisionsChangeVectors[0]
            }

        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string[] Names { get; set; }
        }
    }
}
