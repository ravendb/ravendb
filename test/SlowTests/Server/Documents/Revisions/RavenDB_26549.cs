using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Extensions;
using Raven.Server.Documents;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.Commands;
using Raven.Server.ServerWide;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Revisions
{
    public class RavenDB_26549 : ReplicationTestBase
    {
        public RavenDB_26549(ITestOutputHelper output) : base(output)
        {
        }

        private static readonly string[] SystemCollections =
        {
            CollectionName.HiLoCollection,
            Constants.Documents.Collections.EmbeddingsCacheCollection,
            CdcSinkTaskState.CollectionName,
            EmbeddingsHelper.GetEmbeddingDocumentCollectionName("Posts") // the '@embeddings/*' family, named in the ticket
        };

        // '@'-prefixed, but not on the list - these hold the user's own AI agent transcripts
        private static readonly string[] CollectionsThatLookSystemButAreNot =
        {
            Constants.Documents.Collections.EmptyCollection,
            Constants.Documents.Collections.AiAgentConversationCollection,
            Constants.Documents.Collections.AiAgentConversationHistoryCollection,
            Constants.Documents.Collections.AiAgentConversationDebugCollection
        };

        [RavenFact(RavenTestCategory.Revisions)]
        public void SystemCollectionsAreRecognized()
        {
            foreach (var collection in SystemCollections)
                Assert.True(CollectionName.IsSystemCollection(collection), $"'{collection}' should be a system collection");

            Assert.True(CollectionName.IsSystemCollection("@HILO"), "the list should be case insensitive");
            Assert.True(CollectionName.IsSystemCollection("@EMBEDDINGS/Posts"), "the prefix should be case insensitive");

            // the list is closed, so a leading '@' is not enough - anything we add later has to be listed
            foreach (var collection in CollectionsThatLookSystemButAreNot)
                Assert.False(CollectionName.IsSystemCollection(collection), $"'{collection}' should not be a system collection");

            Assert.False(CollectionName.IsSystemCollection("@whatever-we-add-next"));
            Assert.False(CollectionName.IsSystemCollection("Users"));
            Assert.False(CollectionName.IsSystemCollection(null));
            Assert.False(CollectionName.IsSystemCollection(string.Empty));
        }

        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task DefaultConfigurationDoesNotApplyToSystemCollections(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await SetupRevisionsAsync(store, DefaultOnly());

                foreach (var collection in SystemCollections)
                    await StoreAndModifyAsync(store, IdFor(collection), collection);

                foreach (var collection in CollectionsThatLookSystemButAreNot)
                    await StoreAndModifyAsync(store, IdFor(collection), collection);

                await StoreAndModifyAsync(store, "users/1", "Users");

                using (var session = store.OpenAsyncSession())
                {
                    foreach (var collection in SystemCollections)
                    {
                        var count = await session.Advanced.Revisions.GetCountForAsync(IdFor(collection));
                        Assert.True(0 == count, $"'{collection}' should not be versioned by the default configuration, but got {count} revisions");
                    }

                    // user data is unaffected, the '@'-prefixed collections that are not on the list included
                    Assert.Equal(2, await session.Advanced.Revisions.GetCountForAsync("users/1"));

                    foreach (var collection in CollectionsThatLookSystemButAreNot)
                    {
                        var id = IdFor(collection);

                        var count = await session.Advanced.Revisions.GetCountForAsync(id);
                        Assert.True(2 == count, $"'{collection}' should be versioned by the default configuration, but got {count} revisions");

                        // two revisions would come out of any collection that isn't a system one, so check that
                        // the document really landed in the collection its metadata asked for
                        var document = await session.LoadAsync<User>(id);
                        Assert.Equal(collection, session.Advanced.GetMetadataFor(document).GetString(Constants.Documents.Metadata.Collection));
                    }
                }
            }
        }

        [RavenTheory(RavenTestCategory.Revisions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExplicitPerCollectionConfigurationStillAppliesToSystemCollections(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await SetupRevisionsAsync(store, DefaultWithCdcStatesOptedIn());

                await StoreAndModifyAsync(store, "cdc/1", CdcSinkTaskState.CollectionName);
                await StoreAndModifyAsync(store, "embeddings/1", Constants.Documents.Collections.EmbeddingsCacheCollection);

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Equal(2, await session.Advanced.Revisions.GetCountForAsync("cdc/1"));

                    // the opt-in is per collection, it doesn't leak to the other system collections
                    Assert.Equal(0, await session.Advanced.Revisions.GetCountForAsync("embeddings/1"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task EnforceConfigurationRemovesRevisionsOfSystemCollections()
        {
            using (var store = GetDocumentStore())
            {
                // opting '@cdc-states' in gives us the revisions that databases created before the fix are carrying
                await SetupRevisionsAsync(store, DefaultWithCdcStatesOptedIn());

                await StoreAndModifyAsync(store, "cdc/1", CdcSinkTaskState.CollectionName);
                await StoreAndModifyAsync(store, "users/1", "Users");

                using (var session = store.OpenAsyncSession())
                    Assert.Equal(2, await session.Advanced.Revisions.GetCountForAsync("cdc/1"));

                // drop the opt-in, leaving only the database wide default
                await SetupRevisionsAsync(store, DefaultOnly());

                var database = await Databases.GetDocumentDatabaseInstanceFor(store);
                using (var token = new OperationCancelToken(database.Configuration.Databases.OperationTimeout.AsTimeSpan, database.DatabaseShutdown, CancellationToken.None))
                {
                    await database.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { },
                        new EnforceRevisionsConfigurationOperation.Parameters { IncludeForceCreated = true }, maxOpsPerSecond: null, token);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    Assert.Equal(0, await session.Advanced.Revisions.GetCountForAsync("cdc/1"));
                    Assert.Equal(2, await session.Advanced.Revisions.GetCountForAsync("users/1"));
                }
            }
        }

        // not a theory over database modes: revisions subscriptions are rejected outright on a sharded
        // database (NotSupportedInShardingException, ShardedSubscriptionsHandlerProcessorForPutSubscription.cs:24)
        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.Subscriptions)]
        public async Task RevisionsSubscriptionOnSystemCollectionNamesTheCollection()
        {
            using (var store = GetDocumentStore())
            {
                await SetupRevisionsAsync(store, DefaultOnly());

                await StoreAndModifyAsync(store, "embeddings/1", Constants.Documents.Collections.EmbeddingsCacheCollection);

                var name = await store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = $"from '{Constants.Documents.Collections.EmbeddingsCacheCollection}' (Revisions = true)"
                });

                using (var worker = store.Subscriptions.GetSubscriptionWorker<dynamic>(new SubscriptionWorkerOptions(name)))
                {
                    var run = worker.Run(_ => { });

                    var ex = await Assert.ThrowsAsync<SubscriptionInvalidStateException>(
                        () => run.WaitAndThrowOnTimeout(TimeSpan.FromSeconds(30)));

                    // the message must name the collection rather than claim the database has no
                    // revisions configuration at all - it does have one, just not for this collection
                    Assert.Contains(Constants.Documents.Collections.EmbeddingsCacheCollection, ex.ToString());
                    Assert.DoesNotContain("does not have revisions configuration", ex.ToString());
                }
            }
        }

        // Documents what conflict resolution does to a system collection today. The change on this branch
        // guards only the Configuration.Default branch of GetRevisionsConfiguration, so a document flagged
        // Conflicted/Resolved falls through to ConflictConfiguration instead - it still gets revisions, and
        // they are not trimmed to the user's Default. See the deferred section of the questions doc.
        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ConflictOnSystemCollectionStillCreatesRevisions(Options options)
        {
            const int minToKeep = 2;
            const string userId = "users/1";
            const string systemId = "embeddings/1";

            using var store1 = GetDocumentStore(options);
            using var store2 = GetDocumentStore(options);

            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = minToKeep }
            };
            await SetupRevisionsAsync(store1, configuration);
            await SetupRevisionsAsync(store2, configuration);

            // divergent values on both sides, for a user collection and for a system one
            await StoreOnceAsync(store1, userId, "Users", "remote");
            await StoreOnceAsync(store2, userId, "Users", "local");
            await StoreOnceAsync(store1, systemId, Constants.Documents.Collections.EmbeddingsCacheCollection, "remote");
            await StoreOnceAsync(store2, systemId, Constants.Documents.Collections.EmbeddingsCacheCollection, "local");

            // replicate store1 -> store2, so store2 has to resolve both conflicts
            await SetupReplicationAsync(store1, store2);

            // Wait for a Resolved revision rather than for a revision count. The count climbs and is then
            // trimmed, so it can pass through the value being waited for while resolution is still running,
            // and the assertions below would read a state that is not final. The resolved revision is written
            // and the older ones trimmed in the same operation, so its presence means resolution is done.
            await AssertWaitForTrueAsync(() => HasResolvedRevisionAsync(store2, userId));
            await AssertWaitForTrueAsync(() => HasResolvedRevisionAsync(store2, systemId));

            // the user collection behaves as RavenDB-26296 expects: conflict revisions obey the regular
            // configuration, so they are trimmed down to MinimumRevisionsToKeep
            Assert.Equal(minToKeep, await CountRevisionsAsync(store2, userId));

            // the system collection does not: revisions are created despite the system-collection rule, and
            // they are not trimmed to the user's Default either
            var systemCount = await CountRevisionsAsync(store2, systemId);
            Assert.True(systemCount > minToKeep, $"expected more than {minToKeep} revisions for the system collection, but got {systemCount}");

            using (var session = store2.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                var flags = (await session.Advanced.Revisions.GetMetadataForAsync(systemId))
                    .Select(m => m.GetString(Constants.Documents.Metadata.Flags) ?? string.Empty)
                    .ToList();

                Assert.Equal(3, flags.Count);
                Assert.Contains("Resolved", flags[0]);
                Assert.Contains("Conflicted", flags[1]);
                Assert.Contains("Conflicted", flags[2]);
            }
        }

        // ConfigureRevisionsOperation returns once the Raft command is committed, which is not the same as
        // the database having applied it - so wait for the notification before writing documents that the
        // configuration is supposed to govern. Goes through the store rather than a ServerStore, so it works
        // for a sharded database too.
        private static async Task SetupRevisionsAsync(IDocumentStore store, RevisionsConfiguration configuration)
        {
            var index = await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);
            await store.Maintenance.SendAsync(new WaitForIndexNotificationOperation(index));
        }

        private static async Task<bool> HasResolvedRevisionAsync(IDocumentStore store, string id)
        {
            using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
            {
                var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                return metadata.Any(m => (m.GetString(Constants.Documents.Metadata.Flags) ?? string.Empty).Contains(nameof(DocumentFlags.Resolved)));
            }
        }

        private static async Task<long> CountRevisionsAsync(IDocumentStore store, string id)
        {
            using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                return await session.Advanced.Revisions.GetCountForAsync(id);
        }

        private static async Task StoreOnceAsync(IDocumentStore store, string id, string collection, string name)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = name };
                await session.StoreAsync(user, id);
                session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Collection] = collection;
                await session.SaveChangesAsync();
            }
        }

        private static RevisionsConfiguration DefaultOnly()
        {
            return new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 5 }
            };
        }

        private static RevisionsConfiguration DefaultWithCdcStatesOptedIn()
        {
            var configuration = DefaultOnly();
            configuration.Collections = new Dictionary<string, RevisionsCollectionConfiguration>
            {
                [CdcSinkTaskState.CollectionName] = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 5 }
            };

            return configuration;
        }

        private static string IdFor(string collection) => $"docs/{collection.TrimStart('@')}/1";

        private static async Task StoreAndModifyAsync(IDocumentStore store, string id, string collection)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "original" };
                await session.StoreAsync(user, id);
                session.Advanced.GetMetadataFor(user)[Constants.Documents.Metadata.Collection] = collection;
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(id);
                user.Name = "modified";
                await session.SaveChangesAsync();
            }
        }
    }
}
