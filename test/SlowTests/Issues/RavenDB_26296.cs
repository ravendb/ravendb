using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_26296 : ReplicationTestBase
    {
        public RavenDB_26296(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ConflictRevisionsFollowTheConflictConfiguration_WhenThereIsNoRegularConfiguration(bool conflictRevisionsDisabled)
        {
            const string id = "users/1";

            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            var conflictConfiguration = new RevisionsCollectionConfiguration { Disabled = conflictRevisionsDisabled };
            await RevisionsHelper.SetupConflictedRevisionsAsync(store1, Server.ServerStore, conflictConfiguration);
            await RevisionsHelper.SetupConflictedRevisionsAsync(store2, Server.ServerStore, conflictConfiguration);

            await CreateConflictAndResolveItAsync(store1, store2, id);

            using (var session = store2.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                var flags = await GetRevisionsFlagsAsync(session, id);

                if (conflictRevisionsDisabled)
                {
                    Assert.Empty(flags);
                }
                else
                {
                    Assert.Equal(3, flags.Count);
                    Assert.Contains("Resolved", flags[0]);
                    Assert.Contains("Conflicted", flags[1]);
                    Assert.Contains("Conflicted", flags[2]);
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task ResolvingAConflictLeavesNoRevisions_WhenConflictRevisionsAreDisabled_EvenThoughRegularRevisionsAreEnabled()
        {
            const string id = "users/1";

            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            var disabledConflictConfiguration = new RevisionsCollectionConfiguration { Disabled = true };
            await RevisionsHelper.SetupConflictedRevisionsAsync(store1, Server.ServerStore, disabledConflictConfiguration);
            await RevisionsHelper.SetupConflictedRevisionsAsync(store2, Server.ServerStore, disabledConflictConfiguration);

            var regularConfiguration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            };
            await RevisionsHelper.SetupRevisionsAsync(store2, Server.ServerStore, configuration: regularConfiguration);

            await CreateConflictAndResolveItAsync(store1, store2, id);

            using (var session = store2.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                var flags = await GetRevisionsFlagsAsync(session, id);

                // everything the conflict would have produced is governed by the conflict configuration,
                // including the revision of the document that won - only the pre-conflict one survives
                Assert.Equal(1, flags.Count);
                Assert.DoesNotContain(flags, f => f.Contains("Conflicted"));
                Assert.DoesNotContain(flags, f => f.Contains("Resolved"));
            }
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        public async Task ConflictRevisionsAreSaved_WhenEnabled_AlongsideRegularRevisions()
        {
            const string id = "users/1";

            using var store1 = GetDocumentStore();
            using var store2 = GetDocumentStore();

            var enabledConflictConfiguration = new RevisionsCollectionConfiguration { Disabled = false };
            await RevisionsHelper.SetupConflictedRevisionsAsync(store1, Server.ServerStore, enabledConflictConfiguration);
            await RevisionsHelper.SetupConflictedRevisionsAsync(store2, Server.ServerStore, enabledConflictConfiguration);

            var regularConfiguration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false }
            };
            await RevisionsHelper.SetupRevisionsAsync(store2, Server.ServerStore, configuration: regularConfiguration);

            await CreateConflictAndResolveItAsync(store1, store2, id);

            using (var session = store2.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                var flags = await GetRevisionsFlagsAsync(session, id);

                Assert.Equal(3, flags.Count);
                Assert.Contains("Resolved", flags[0]);
                Assert.Contains(flags, f => f.Contains("Conflicted"));
            }
        }

        private async Task CreateConflictAndResolveItAsync(IDocumentStore store1, IDocumentStore store2, string id)
        {
            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "remote" }, id);
                await session.SaveChangesAsync();
            }

            using (var session = store2.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "local" }, id);
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(store1, store2);

            // the assertions below are only meaningful once the conflict has actually been resolved,
            // which happens asynchronously after the replication reaches store2
            await AssertWaitForTrueAsync(async () =>
            {
                using (var session = store2.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    var user = await session.LoadAsync<User>(id);
                    if (user == null)
                        return false;

                    var flags = session.Advanced.GetMetadataFor(user).GetString(Constants.Documents.Metadata.Flags) ?? string.Empty;
                    return flags.Contains("Resolved");
                }
            });
        }

        private static async Task<List<string>> GetRevisionsFlagsAsync(IAsyncDocumentSession session, string id)
        {
            var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
            return metadata.Select(m => m.GetString(Constants.Documents.Metadata.Flags) ?? string.Empty).ToList();
        }
    }
}
