using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Server.Documents;
using Raven.Server.Documents.Operations;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Revisions
{
    public class RevisionsStartFromEtagTests : ReplicationTestBase
    {
        public RevisionsStartFromEtagTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task EnforceRevisionConfiguration_WithStartFromEtag_SkipsDocumentsCreatedAfterEtag()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // Create companies/1 with 10 revisions (these will have lower etags)
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"Company {i}" }, "companies/1");
                        await session.SaveChangesAsync();
                    }
                }

                // Record the etag boundary -- everything created before this is "in scope"
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var startFromEtag = db.DocumentsStorage.GenerateNextEtag();

                // Create companies/2 with 10 revisions AFTER the etag boundary
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"Company {i}" }, "companies/2");
                        await session.SaveChangesAsync();
                    }
                }

                // Reduce config so enforcement would trim revisions
                configuration.Default.MinimumRevisionsToKeep = 2;
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                EnforceConfigurationResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    var etagBarrier = db.DocumentsStorage.GenerateNextEtag();
                    result = (EnforceConfigurationResult)await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(
                        _ => { }, new EnforceRevisionsConfigurationOperation.Parameters
                        {
                            IncludeForceCreated = false,
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long> { [db.Name] = startFromEtag },
                                EtagBarriers = new Dictionary<string, long> { [db.Name] = etagBarrier },
                                NodeTags = new Dictionary<string, string> { [db.Name] = db.ServerStore.NodeTag }
                            }
                        }, token);
                }

                using (var session = store.OpenAsyncSession(new Raven.Client.Documents.Session.SessionOptions { NoCaching = true }))
                {
                    // companies/1 was in scope -- its revisions should be trimmed to 2
                    var revisions1 = await session.Advanced.Revisions.GetMetadataForAsync("companies/1");
                    Assert.Equal(2, revisions1.Count);

                    // companies/2 was created after startFromEtag -- enforce should not have touched it
                    var revisions2 = await session.Advanced.Revisions.GetMetadataForAsync("companies/2");
                    Assert.Equal(10, revisions2.Count);
                }

                // LastProcessedEtags must be set and within the scanned range
                Assert.True(result.LastProcessedEtags.ContainsKey(store.Database) && result.LastProcessedEtags[store.Database] > 0);
                Assert.True(result.LastProcessedEtags[store.Database] <= startFromEtag);
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task AdoptOrphanedRevisions_WithStartFromEtag_SkipsDocumentsCreatedAfterEtag()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // Create user1 with several revisions, then delete it (creates a delete revision)
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "User1" }, "users/1-A");
                    await session.SaveChangesAsync();
                    for (int i = 0; i < 3; i++)
                    {
                        var u = await session.LoadAsync<User>("users/1-A");
                        u.Name = $"User1-v{i}";
                        await session.SaveChangesAsync();
                    }
                    session.Delete("users/1-A");
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Orphan user1 by removing its delete revision
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    db.DocumentsStorage.RevisionsStorage.ForTestingPurposesOnly().DeleteLastRevisionFor(context, "users/1-A", "Users");
                    tx.Commit();
                }

                // Verify user1 is now orphaned (its most recent revision is not a delete revision)
                using (var session = store.OpenAsyncSession())
                {
                    var meta1 = await session.Advanced.Revisions.GetMetadataForAsync("users/1-A");
                    Assert.False(meta1[0].GetString(Constants.Documents.Metadata.Flags).Contains(DocumentFlags.DeleteRevision.ToString()));
                }

                // Record the etag boundary -- user2 will be created entirely after this
                var startFromEtag = db.DocumentsStorage.GenerateNextEtag();

                // Create user2 with several revisions, then delete it
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "User2" }, "users/2-B");
                    await session.SaveChangesAsync();
                    for (int i = 0; i < 3; i++)
                    {
                        var u = await session.LoadAsync<User>("users/2-B");
                        u.Name = $"User2-v{i}";
                        await session.SaveChangesAsync();
                    }
                    session.Delete("users/2-B");
                    await session.SaveChangesAsync();
                }

                // Orphan user2 by removing its delete revision
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    db.DocumentsStorage.RevisionsStorage.ForTestingPurposesOnly().DeleteLastRevisionFor(context, "users/2-B", "Users");
                    tx.Commit();
                }

                // Verify user2 is orphaned too
                using (var session = store.OpenAsyncSession())
                {
                    var meta2 = await session.Advanced.Revisions.GetMetadataForAsync("users/2-B");
                    Assert.False(meta2[0].GetString(Constants.Documents.Metadata.Flags).Contains(DocumentFlags.DeleteRevision.ToString()));
                }

                // Run adopt with startFromEtag -- should only fix user1 (all revisions below the barrier)
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    var etagBarrier = db.DocumentsStorage.GenerateNextEtag();
                    await db.DocumentsStorage.RevisionsStorage.AdoptOrphanedAsync(
                        onProgress: null, new AdoptOrphanedRevisionsOperation.Parameters
                        {
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long> { [db.Name] = startFromEtag },
                                EtagBarriers = new Dictionary<string, long> { [db.Name] = etagBarrier },
                                NodeTags = new Dictionary<string, string> { [db.Name] = db.ServerStore.NodeTag }
                            }
                        }, token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    // user1 was below startFromEtag -- its delete revision should be recreated
                    var meta1 = await session.Advanced.Revisions.GetMetadataForAsync("users/1-A");
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), meta1[0].GetString(Constants.Documents.Metadata.Flags));

                    // user2 was created entirely after startFromEtag -- should still be orphaned
                    var meta2 = await session.Advanced.Revisions.GetMetadataForAsync("users/2-B");
                    Assert.False(meta2[0].GetString(Constants.Documents.Metadata.Flags).Contains(DocumentFlags.DeleteRevision.ToString()));
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevertRevisions_WithStartFromEtag_SkipsDocumentsCreatedAfterEtag()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);

                // Create companies/1 at a known point in time
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "Original1" }, "companies/1");
                    await session.SaveChangesAsync();
                }

                var beforeTime = DateTime.UtcNow;

                // Update companies/1 -- creates a revision after the target revert time
                using (var session = store.OpenAsyncSession())
                {
                    var c = await session.LoadAsync<Company>("companies/1");
                    c.Name = "Updated1";
                    await session.SaveChangesAsync();
                }

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Record etag barrier -- companies/2 will be created entirely after this
                var startFromEtag = db.DocumentsStorage.GenerateNextEtag();

                // Create and update companies/2 AFTER the etag barrier
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "Original2" }, "companies/2");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    var c = await session.LoadAsync<Company>("companies/2");
                    c.Name = "Updated2";
                    await session.SaveChangesAsync();
                }

                // Revert with startFromEtag -- companies/2 is entirely above the barrier, should be skipped
                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    var etagBarrier = db.DocumentsStorage.GenerateNextEtag();
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(
                        new RevertRevisionsRequest
                        {
                            Time = beforeTime,
                            WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds,
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long> { [db.Name] = startFromEtag },
                                EtagBarriers = new Dictionary<string, long> { [db.Name] = etagBarrier },
                                NodeTags = new Dictionary<string, string> { [db.Name] = db.ServerStore.NodeTag }
                            }
                        }, onProgress: null, token);
                }

                using (var session = store.OpenAsyncSession())
                {
                    // companies/1 was in scope -- should be reverted to its state at beforeTime ("Original1")
                    var company1 = await session.LoadAsync<Company>("companies/1");
                    Assert.Equal("Original1", company1.Name);

                    // companies/2 was created entirely after startFromEtag -- should remain as "Updated2"
                    var company2 = await session.LoadAsync<Company>("companies/2");
                    Assert.Equal("Updated2", company2.Name);
                }

                Assert.Equal(1, result.RevertedDocuments);
                Assert.True(result.LastProcessedEtags.ContainsKey(store.Database) && result.LastProcessedEtags[store.Database] > 0);
                Assert.True(result.LastProcessedEtags[store.Database] <= startFromEtag);
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevertRevisions_Resume_DoesNotDoubleRevert()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Create companies/1 "Original1" -- this revision gets a LOW etag
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "Original1" }, "companies/1");
                    await session.SaveChangesAsync();
                }

                var beforeTime = DateTime.UtcNow;

                // Capture mid-point etag BEFORE the update -- "Original1" revision is below this
                var midEtag = db.DocumentsStorage.GenerateNextEtag();

                // Update companies/1 -- this revision gets a HIGH etag (above midEtag)
                using (var session = store.OpenAsyncSession())
                {
                    var c = await session.LoadAsync<Company>("companies/1");
                    c.Name = "Updated1";
                    await session.SaveChangesAsync();
                }

                // FIRST RUN: full revert (startFromEtag = null -- scans from current max down to etag 1)
                // The scan hits "Updated1" revision (high etag) first, reverts companies/1 to "Original1"
                // Then hits "Original1" revision (low etag) but ScannedIds deduplication skips it
                RevertResult firstResult;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    firstResult = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(
                        beforeTime, TimeSpan.FromMinutes(60), onProgress: null, token);
                }

                Assert.Equal(1, firstResult.RevertedDocuments);

                // companies/1 is now "Original1" with a new etag (> midEtag) and flagged Reverted
                string changeVectorAfterFirstRun;
                using (var session = store.OpenAsyncSession(new Raven.Client.Documents.Session.SessionOptions { NoCaching = true }))
                {
                    var c = await session.LoadAsync<Company>("companies/1");
                    Assert.Equal("Original1", c.Name);
                    changeVectorAfterFirstRun = session.Advanced.GetChangeVectorFor(c);
                }

                // RESUME: startFromEtag = midEtag -- scans only the LOW etag range
                // Pass the original etagBarrier from run 1 so the barrier check in RevertDocumentsCommand
                // correctly identifies companies/1 (currentEtag > originalBarrier && Reverted) and skips it.
                RevertResult resumeResult;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                        resumeResult = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(
                        new RevertRevisionsRequest
                        {
                            Time = beforeTime,
                            WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds,
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long> { [db.Name] = midEtag },
                                EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                                NodeTags = new Dictionary<string, string>(firstResult.NodeTags)
                            }
                        }, onProgress: null, token);
                }

                using (var session = store.OpenAsyncSession(new Raven.Client.Documents.Session.SessionOptions { NoCaching = true }))
                {
                    var c = await session.LoadAsync<Company>("companies/1");
                    // Content must still be correct
                    Assert.Equal("Original1", c.Name);
                    // Change vector must be unchanged -- no new write happened despite the scan visiting the document
                    Assert.Equal(changeVectorAfterFirstRun, session.Advanced.GetChangeVectorFor(c));
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task EnforceRevisionConfiguration_Resume_IsIdempotent()
        {
            using (var store = GetDocumentStore())
            {
                // Setup: keep at most 2 revisions
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // Create 10 revisions for companies/1
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"v{i}" }, "companies/1");
                        await session.SaveChangesAsync();
                    }
                }

                // Tighten limit to 2 -- first run will trim 8 revisions
                configuration.Default.MinimumRevisionsToKeep = 2;
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                var baseParams = new EnforceRevisionsConfigurationOperation.Parameters { IncludeForceCreated = false };

                // FIRST RUN via operation API
                var op1 = await store.Operations.SendAsync(new EnforceRevisionsConfigurationOperation(baseParams));
                var firstResult = (EnforceConfigurationResult)await op1.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.True(firstResult.RemovedRevisions > 0, "First run should have trimmed revisions");
                Assert.Equal(1, firstResult.ScannedDocuments);

                using (var session = store.OpenAsyncSession(new Raven.Client.Documents.Session.SessionOptions { NoCaching = true }))
                {
                    var revisions = await session.Advanced.Revisions.GetMetadataForAsync("companies/1");
                    Assert.Equal(2, revisions.Count);
                }

                // RESUME: manually set ContinuationParameters from first result
                var resumeParams = new EnforceRevisionsConfigurationOperation.Parameters
                {
                    IncludeForceCreated = baseParams.IncludeForceCreated,
                    ContinuationParameters = new RevisionsOperationContinuationParameters
                    {
                        StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                        EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                        NodeTags = new Dictionary<string, string>(firstResult.NodeTags)
                    }
                };
                var op2 = await store.Operations.SendAsync(new EnforceRevisionsConfigurationOperation(resumeParams));
                var resumeResult = (EnforceConfigurationResult)await op2.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                // No additional trimming should have occurred -- the state is already correct
                Assert.Equal(0, resumeResult.RemovedRevisions);

                using (var session = store.OpenAsyncSession(new Raven.Client.Documents.Session.SessionOptions { NoCaching = true }))
                {
                    var revisions = await session.Advanced.Revisions.GetMetadataForAsync("companies/1");
                    Assert.Equal(2, revisions.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task AdoptOrphanedRevisions_Resume_IsIdempotent()
        {
            using (var store = GetDocumentStore())
            {
                var db = await Databases.GetDocumentDatabaseInstanceFor(store);
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // Create users/1-A and delete it
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "User1" }, "users/1-A");
                    await session.SaveChangesAsync();
                    session.Delete("users/1-A");
                    await session.SaveChangesAsync();
                }

                // Orphan users/1-A by removing its delete revision
                using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (var tx = context.OpenWriteTransaction())
                {
                    db.DocumentsStorage.RevisionsStorage.ForTestingPurposesOnly().DeleteLastRevisionFor(context, "users/1-A", "Users");
                    tx.Commit();
                }

                var baseParams = new AdoptOrphanedRevisionsOperation.Parameters();

                // FIRST RUN via operation API -- should fix users/1-A
                var op1 = await store.Operations.SendAsync(new AdoptOrphanedRevisionsOperation(baseParams));
                var firstResult = (AdoptOrphanedRevisionsResult)await op1.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                Assert.Equal(1, firstResult.AdoptedCount);

                // users/1-A now has a delete revision
                using (var session = store.OpenAsyncSession())
                {
                    var meta = await session.Advanced.Revisions.GetMetadataForAsync("users/1-A");
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), meta[0].GetString(Constants.Documents.Metadata.Flags));
                }

                // RESUME: manually set ContinuationParameters from first result
                // users/1-A's revision is still in the scan range but it is already adopted
                var resumeParams = new AdoptOrphanedRevisionsOperation.Parameters
                {
                    ContinuationParameters = new RevisionsOperationContinuationParameters
                    {
                        StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                        EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                        NodeTags = new Dictionary<string, string>(firstResult.NodeTags)
                    }
                };
                var op2 = await store.Operations.SendAsync(new AdoptOrphanedRevisionsOperation(resumeParams));
                var resumeResult = (AdoptOrphanedRevisionsResult)await op2.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                // No additional adoption should have occurred
                Assert.Equal(0, resumeResult.AdoptedCount);

                using (var session = store.OpenAsyncSession())
                {
                    var meta = await session.Advanced.Revisions.GetMetadataForAsync("users/1-A");
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), meta[0].GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }


        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task EnforceRevisionConfiguration_WithStartFromEtag_Sharded(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // Create companies/1 and companies/2 -- they land on different shards
                Assert.NotEqual(await Sharding.GetShardNumberForAsync(store, "companies/1"),
                    await Sharding.GetShardNumberForAsync(store, "companies/2"));

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"c1-v{i}" }, "companies/1");
                        await session.StoreAsync(new Company { Name = $"c2-v{i}" }, "companies/2");
                        await session.SaveChangesAsync();
                    }
                }

                // Capture per-shard starting etags as the boundary
                var startFromEtags = new Dictionary<string, long>();
                var etagBarriers = new Dictionary<string, long>();
                var nodeTags = new Dictionary<string, string>();
                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                {
                    startFromEtags[shard.Name] = shard.DocumentsStorage.GenerateNextEtag();
                    etagBarriers[shard.Name] = startFromEtags[shard.Name];
                    nodeTags[shard.Name] = shard.ServerStore.NodeTag;
                }

                // Create companies/3 (all revisions will be above the boundary)
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"c3-v{i}" }, "companies/3");
                        await session.SaveChangesAsync();
                    }
                }

                // Tighten config so enforcement would trim
                configuration.Default.MinimumRevisionsToKeep = 2;
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                var op = await store.Operations.SendAsync(
                    new EnforceRevisionsConfigurationOperation(new EnforceRevisionsConfigurationOperation.Parameters
                    {
                        ContinuationParameters = new RevisionsOperationContinuationParameters { StartFromEtags = startFromEtags, EtagBarriers = etagBarriers, NodeTags = nodeTags }
                    }));
                var result = (EnforceConfigurationResult)await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    // companies/1 and companies/2 were below the boundary -- revisions should be trimmed
                    var revisions1 = await session.Advanced.Revisions.GetMetadataForAsync("companies/1");
                    Assert.Equal(2, revisions1.Count);
                    var revisions2 = await session.Advanced.Revisions.GetMetadataForAsync("companies/2");
                    Assert.Equal(2, revisions2.Count);

                    // companies/3 was created entirely after startFromEtag -- should not be touched
                    var revisions3 = await session.Advanced.Revisions.GetMetadataForAsync("companies/3");
                    Assert.Equal(10, revisions3.Count);
                }

                Assert.True(result.LastProcessedEtags.Count > 0, "LastProcessedEtags should be populated for each shard");
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task AdoptOrphanedRevisions_WithStartFromEtag_Sharded(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 100 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // users/1-A and users/2-B must land on different shards to exercise cross-shard forwarding
                Assert.NotEqual(await Sharding.GetShardNumberForAsync(store, "users/1-A"),
                    await Sharding.GetShardNumberForAsync(store, "users/2-B"));

                async Task OrphanDocAsync(string id, string collection)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User { Name = id }, id);
                        await session.SaveChangesAsync();
                        session.Delete(id);
                        await session.SaveChangesAsync();
                    }
                    await foreach (var shard in await Sharding.GetShardsDocumentDatabaseInstancesForDocId(store, id))
                    {
                        using (shard.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                        using (var tx = ctx.OpenWriteTransaction())
                        {
                            shard.DocumentsStorage.RevisionsStorage.ForTestingPurposesOnly().DeleteLastRevisionFor(ctx, id, collection);
                            tx.Commit();
                        }
                    }
                }

                await OrphanDocAsync("users/1-A", "Users");
                await OrphanDocAsync("users/2-B", "Users");

                bool IsOrphaned(List<IMetadataDictionary> meta) =>
                    meta.Count > 0 && meta[0].GetString(Constants.Documents.Metadata.Flags).Contains(DocumentFlags.DeleteRevision.ToString()) == false;

                // Build per-shard dict with etag 0 -- since etag barrier = 0, all revisions (etag > 0) are skipped
                var zeroEtags = new Dictionary<string, long>();
                var zeroBarriers = new Dictionary<string, long>();
                var adoptNodeTags = new Dictionary<string, string>();
                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                {
                    zeroEtags[shard.Name] = 0;
                    zeroBarriers[shard.Name] = 0;
                    adoptNodeTags[shard.Name] = shard.ServerStore.NodeTag;
                }

                var op = await store.Operations.SendAsync(
                    new AdoptOrphanedRevisionsOperation(new AdoptOrphanedRevisionsOperation.Parameters { ContinuationParameters = new RevisionsOperationContinuationParameters { StartFromEtags = zeroEtags, EtagBarriers = zeroBarriers, NodeTags = adoptNodeTags } }));
                var result = (AdoptOrphanedRevisionsResult)await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var session = store.OpenAsyncSession())
                {
                    Assert.True(IsOrphaned(await session.Advanced.Revisions.GetMetadataForAsync("users/1-A")), "users/1-A should still be orphaned (StartFromEtags all 0)");
                    Assert.True(IsOrphaned(await session.Advanced.Revisions.GetMetadataForAsync("users/2-B")), "users/2-B should still be orphaned (StartFromEtags all 0)");
                }

                // StartFromEtags = null processes everything; both should be adopted
                op = await store.Operations.SendAsync(
                    new AdoptOrphanedRevisionsOperation(new AdoptOrphanedRevisionsOperation.Parameters { ContinuationParameters = null }));
                result = (AdoptOrphanedRevisionsResult)await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var session = store.OpenAsyncSession())
                {
                    var meta1 = await session.Advanced.Revisions.GetMetadataForAsync("users/1-A");
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), meta1[0].GetString(Constants.Documents.Metadata.Flags));
                    var meta2 = await session.Advanced.Revisions.GetMetadataForAsync("users/2-B");
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), meta2[0].GetString(Constants.Documents.Metadata.Flags));
                }

                Assert.True(result.LastProcessedEtags.Count > 0, "LastProcessedEtags should be populated for each shard");
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task RevertRevisions_WithStartFromEtag_Sharded(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                await RevisionsHelper.SetupRevisionsAsync(store);

                // companies/1 and companies/2 must be on different shards to exercise cross-shard forwarding
                Assert.NotEqual(await Sharding.GetShardNumberForAsync(store, "companies/1"),
                    await Sharding.GetShardNumberForAsync(store, "companies/2"));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "Original1" }, "companies/1");
                    await session.StoreAsync(new Company { Name = "Original2" }, "companies/2");
                    await session.SaveChangesAsync();
                }

                var beforeTime = DateTime.UtcNow;

                using (var session = store.OpenAsyncSession())
                {
                    var c1 = await session.LoadAsync<Company>("companies/1");
                    var c2 = await session.LoadAsync<Company>("companies/2");
                    c1.Name = "Updated1";
                    c2.Name = "Updated2";
                    await session.SaveChangesAsync();
                }

                // Build per-shard dict with etag 0 -- since etagBarrier = 0, no revisions are processed
                var zeroEtags = new Dictionary<string, long>();
                var zeroBarriers = new Dictionary<string, long>();
                var revertNodeTags = new Dictionary<string, string>();
                await foreach (var shard in Sharding.GetShardsDocumentDatabaseInstancesFor(store))
                {
                    zeroEtags[shard.Name] = 0;
                    zeroBarriers[shard.Name] = 0;
                    revertNodeTags[shard.Name] = shard.ServerStore.NodeTag;
                }

                var op = await store.Maintenance.SendAsync(new RevertRevisionsOperation(
                    new RevertRevisionsRequest { Time = beforeTime, WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds, ContinuationParameters = new RevisionsOperationContinuationParameters { StartFromEtags = zeroEtags, EtagBarriers = zeroBarriers, NodeTags = revertNodeTags } }));
                var result = (RevertResult)await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    var c1 = await session.LoadAsync<Company>("companies/1");
                    var c2 = await session.LoadAsync<Company>("companies/2");
                    Assert.Equal("Updated1", c1.Name);
                    Assert.Equal("Updated2", c2.Name);
                }
                Assert.Equal(0, result.RevertedDocuments);

                // StartFromEtags = null processes everything; both should be reverted
                op = await store.Maintenance.SendAsync(new RevertRevisionsOperation(
                    new RevertRevisionsRequest { Time = beforeTime, WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds, ContinuationParameters = null }));
                result = (RevertResult)await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    var c1 = await session.LoadAsync<Company>("companies/1");
                    var c2 = await session.LoadAsync<Company>("companies/2");
                    Assert.Equal("Original1", c1.Name);
                    Assert.Equal("Original2", c2.Name);
                }

                Assert.Equal(2, result.RevertedDocuments);
                Assert.True(result.LastProcessedEtags.Count > 0, "LastProcessedEtags should be populated for each shard");
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task EnforceRevisionConfiguration_ReportsLastProcessedEtag()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                for (int i = 0; i < 5; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"Company {i}" }, "companies/1");
                        await session.SaveChangesAsync();
                    }
                }

                // Reduce config so some revisions get removed during enforce
                configuration.Default.MinimumRevisionsToKeep = 2;
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                EnforceConfigurationResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (EnforceConfigurationResult)await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(
                        _ => { }, new EnforceRevisionsConfigurationOperation.Parameters { IncludeForceCreated = false }, token);
                }

                Assert.Equal(1, result.ScannedDocuments);
                Assert.True(result.RemovedRevisions > 0);
                Assert.True(result.LastProcessedEtags.ContainsKey(store.Database) && result.LastProcessedEtags[store.Database] > 0, "LastProcessedEtags should be set when revisions were scanned");
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task EnforceRevisionConfiguration_Resume_DoesNotSkipUnprocessedCollections()
        {
            // Scenario: collections A (Companies), B (Users), C (Orders) are processed sequentially.
            // Collection A completes, B is partially processed (simulated), C never started.
            // Resuming with LastProcessedEtags from that partial run must still enforce C fully.

            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Create 10 revisions each for companies/1, users/1, orders/1
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"Company {i}" }, "companies/1");
                        await session.StoreAsync(new User { Name = $"User {i}" }, "users/1");
                        await session.StoreAsync(new Order { Company = $"Order {i}" }, "orders/1");
                        await session.SaveChangesAsync();
                    }
                }

                // Tighten config to keep only 2 revisions
                configuration.Default.MinimumRevisionsToKeep = 2;
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // Step 1: Run enforce on Companies only — simulates "collection A completed"
                EnforceConfigurationResult firstResult;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    firstResult = (EnforceConfigurationResult)await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(
                        _ => { },
                        new EnforceRevisionsConfigurationOperation.Parameters
                        {
                            IncludeForceCreated = false,
                            Collections = new[] { "Companies" }
                        },
                        token);
                }

                Assert.True(firstResult.RemovedRevisions > 0, "First run should have trimmed Companies revisions");

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetMetadataForAsync("companies/1");
                    Assert.Equal(2, companiesRevisions.Count);

                    // Users and Orders untouched
                    var usersRevisions = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.Equal(10, usersRevisions.Count);
                    var ordersRevisions = await session.Advanced.Revisions.GetMetadataForAsync("orders/1");
                    Assert.Equal(10, ordersRevisions.Count);
                }

                // Step 2: Simulate "resume" using firstResult's etags, but now targeting all 3 collections.
                // This simulates: A was done, B was partially done (at the etag boundary from A's run), C never started.
                // The bug: Orders (collection C) revisions above LastProcessedEtags will be skipped.
                EnforceConfigurationResult resumeResult;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    resumeResult = (EnforceConfigurationResult)await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(
                        _ => { },
                        new EnforceRevisionsConfigurationOperation.Parameters
                        {
                            IncludeForceCreated = false,
                            Collections = new[] { "Companies", "Users", "Orders" },
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                                EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                                NodeTags = new Dictionary<string, string>(firstResult.NodeTags)
                            }
                        },
                        token);
                }

                // The resumed run must have trimmed Users and Orders
                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetMetadataForAsync("companies/1");
                    Assert.Equal(2, companiesRevisions.Count);

                    var usersRevisions = await session.Advanced.Revisions.GetMetadataForAsync("users/1");
                    Assert.Equal(2, usersRevisions.Count); // This will fail — Users revisions above the resume etag are skipped

                    var ordersRevisions = await session.Advanced.Revisions.GetMetadataForAsync("orders/1");
                    Assert.Equal(2, ordersRevisions.Count); // This will also fail — Orders never processed above the resume etag
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevertRevisions_Resume_DoesNotSkipUnprocessedCollections()
        {
            // Scenario: collections A (Companies), B (Users), C (Orders) are processed sequentially.
            // Collection A completes, B is partially processed (simulated), C never started.
            // Resuming with LastProcessedEtags from that partial run must still revert C fully.

            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Create original documents in all 3 collections
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "OriginalCompany" }, "companies/1");
                    await session.StoreAsync(new User { Name = "OriginalUser" }, "users/1");
                    await session.StoreAsync(new Order { Company = "OriginalOrder" }, "orders/1");
                    await session.SaveChangesAsync();
                }

                var beforeTime = DateTime.UtcNow;

                // Update all documents after the revert point
                using (var session = store.OpenAsyncSession())
                {
                    var c = await session.LoadAsync<Company>("companies/1");
                    var u = await session.LoadAsync<User>("users/1");
                    var o = await session.LoadAsync<Order>("orders/1");
                    c.Name = "UpdatedCompany";
                    u.Name = "UpdatedUser";
                    o.Company = "UpdatedOrder";
                    await session.SaveChangesAsync();
                }

                // Step 1: Revert only Companies — simulates "collection A completed"
                RevertResult firstResult;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    firstResult = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(
                        new RevertRevisionsRequest
                        {
                            Time = beforeTime,
                            WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds,
                            Collections = new[] { "Companies" }
                        },
                        onProgress: null, token);
                }

                Assert.Equal(1, firstResult.RevertedDocuments);

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    var company = await session.LoadAsync<Company>("companies/1");
                    Assert.Equal("OriginalCompany", company.Name);

                    // Users and Orders should still be updated
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("UpdatedUser", user.Name);
                    var order = await session.LoadAsync<Order>("orders/1");
                    Assert.Equal("UpdatedOrder", order.Company);
                }

                // Step 2: Resume with firstResult's etags, targeting all 3 collections.
                // This simulates: A was done, B partially done, C never started.
                // The bug: Orders (collection C) revisions above LastProcessedEtags will be skipped.
                RevertResult resumeResult;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    resumeResult = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(
                        new RevertRevisionsRequest
                        {
                            Time = beforeTime,
                            WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds,
                            Collections = new[] { "Companies", "Users", "Orders" },
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                                EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                                NodeTags = new Dictionary<string, string>(firstResult.NodeTags)
                            }
                        },
                        onProgress: null, token);
                }

                // The resumed run must have reverted Users and Orders
                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    var company = await session.LoadAsync<Company>("companies/1");
                    Assert.Equal("OriginalCompany", company.Name);

                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("OriginalUser", user.Name); // Will fail if Users revisions above resume etag are skipped

                    var order = await session.LoadAsync<Order>("orders/1");
                    Assert.Equal("OriginalOrder", order.Company); // Will fail if Orders never processed above resume etag
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevertRevisions_WithMultipleCollections_RevertsAll()
        {
            // Verify that a fresh (non-resumed) revert with explicit collections reverts all of them.

            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                // Create original documents in 3 collections
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "OriginalCompany" }, "companies/1");
                    await session.StoreAsync(new User { Name = "OriginalUser" }, "users/1");
                    await session.StoreAsync(new Order { Company = "OriginalOrder" }, "orders/1");
                    await session.SaveChangesAsync();
                }

                var beforeTime = DateTime.UtcNow;

                // Update all documents after the revert point
                using (var session = store.OpenAsyncSession())
                {
                    var c = await session.LoadAsync<Company>("companies/1");
                    var u = await session.LoadAsync<User>("users/1");
                    var o = await session.LoadAsync<Order>("orders/1");
                    c.Name = "UpdatedCompany";
                    u.Name = "UpdatedUser";
                    o.Company = "UpdatedOrder";
                    await session.SaveChangesAsync();
                }

                // Revert all 3 collections in a single operation (no resume, no continuation)
                RevertResult result;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(
                        new RevertRevisionsRequest
                        {
                            Time = beforeTime,
                            WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds,
                            Collections = new[] { "Companies", "Users", "Orders" }
                        },
                        onProgress: null, token);
                }

                Assert.Equal(3, result.RevertedDocuments);

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true }))
                {
                    var company = await session.LoadAsync<Company>("companies/1");
                    Assert.Equal("OriginalCompany", company.Name);

                    var user = await session.LoadAsync<User>("users/1");
                    Assert.Equal("OriginalUser", user.Name);

                    var order = await session.LoadAsync<Order>("orders/1");
                    Assert.Equal("OriginalOrder", order.Company);
                }
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task EnforceRevisionConfiguration_NodeTagValidation_RejectsWrongNode()
        {
            // Non-sharded: verify that server-side validation rejects resume on wrong node
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                for (int i = 0; i < 5; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"Company {i}" }, "companies/1");
                        await session.SaveChangesAsync();
                    }
                }

                configuration.Default.MinimumRevisionsToKeep = 2;
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // First run — produces NodeTags in the result
                EnforceConfigurationResult firstResult;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    firstResult = (EnforceConfigurationResult)await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(
                        _ => { },
                        new EnforceRevisionsConfigurationOperation.Parameters { IncludeForceCreated = false },
                        token);
                }

                Assert.True(firstResult.NodeTags.ContainsKey(store.Database), "NodeTags should contain an entry for the database");
                Assert.Equal(db.ServerStore.NodeTag, firstResult.NodeTags[store.Database]);

                // Resume with correct NodeTags — should succeed
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(
                        _ => { },
                        new EnforceRevisionsConfigurationOperation.Parameters
                        {
                            IncludeForceCreated = false,
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                                EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                                NodeTags = new Dictionary<string, string>(firstResult.NodeTags)
                            }
                        },
                        token);
                }

                // Resume with wrong NodeTags — should throw InvalidOperationException
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    using var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None);
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(
                        _ => { },
                        new EnforceRevisionsConfigurationOperation.Parameters
                        {
                            IncludeForceCreated = false,
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                                EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                                NodeTags = new Dictionary<string, string> { [store.Database] = "Z" } // fake node tag
                            }
                        },
                        token);
                });

                Assert.Contains("node 'Z'", ex.Message);
                Assert.Contains("ForNode", ex.Message);
            }
        }

        [RavenFact(RavenTestCategory.Revisions)]
        public async Task RevertRevisions_NodeTagValidation_RejectsWrongNode()
        {
            // Non-sharded: verify that server-side validation rejects resume on wrong node for revert
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisionsAsync(store);

                var db = await Databases.GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company { Name = "Original" }, "companies/1");
                    await session.SaveChangesAsync();
                }

                var beforeTime = DateTime.UtcNow;

                using (var session = store.OpenAsyncSession())
                {
                    var c = await session.LoadAsync<Company>("companies/1");
                    c.Name = "Updated";
                    await session.SaveChangesAsync();
                }

                // First run — get NodeTags
                RevertResult firstResult;
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                {
                    firstResult = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(
                        new RevertRevisionsRequest { Time = beforeTime, WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds },
                        onProgress: null, token);
                }

                Assert.True(firstResult.NodeTags.ContainsKey(store.Database));
                Assert.Equal(db.ServerStore.NodeTag, firstResult.NodeTags[store.Database]);

                // Resume with wrong NodeTags — should throw
                var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    using var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None);
                    await db.DocumentsStorage.RevisionsStorage.RevertRevisions(
                        new RevertRevisionsRequest
                        {
                            Time = beforeTime,
                            WindowInSec = (long)TimeSpan.FromMinutes(60).TotalSeconds,
                            ContinuationParameters = new RevisionsOperationContinuationParameters
                            {
                                StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                                EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                                NodeTags = new Dictionary<string, string> { [store.Database] = "Z" }
                            }
                        },
                        onProgress: null, token);
                });

                Assert.Contains("node 'Z'", ex.Message);
                Assert.Contains("ForNode", ex.Message);
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task EnforceRevisionConfiguration_NodeTagValidation_Sharded(Options options)
        {
            // Sharded: verify that NodeTags are populated per shard and validation works
            using (var store = GetDocumentStore(options))
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // companies/1 and companies/2 land on different shards
                Assert.NotEqual(await Sharding.GetShardNumberForAsync(store, "companies/1"),
                    await Sharding.GetShardNumberForAsync(store, "companies/2"));

                for (int i = 0; i < 5; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"c1-v{i}" }, "companies/1");
                        await session.StoreAsync(new Company { Name = $"c2-v{i}" }, "companies/2");
                        await session.SaveChangesAsync();
                    }
                }

                configuration.Default.MinimumRevisionsToKeep = 2;
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // First run
                var op = await store.Operations.SendAsync(
                    new EnforceRevisionsConfigurationOperation(new EnforceRevisionsConfigurationOperation.Parameters { IncludeForceCreated = false }));
                var firstResult = (EnforceConfigurationResult)await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                // NodeTags should have entries for each shard
                Assert.True(firstResult.NodeTags.Count > 0, "NodeTags should be populated for sharded database");
                Assert.True(firstResult.LastProcessedEtags.Count > 0);

                // Every shard that has LastProcessedEtags should also have a NodeTag
                foreach (var shardName in firstResult.LastProcessedEtags.Keys)
                {
                    Assert.True(firstResult.NodeTags.ContainsKey(shardName),
                        $"NodeTags should contain entry for shard '{shardName}'");
                }

                // Resume with correct NodeTags — should succeed
                var resumeParams = new EnforceRevisionsConfigurationOperation.Parameters
                {
                    IncludeForceCreated = false,
                    ContinuationParameters = new RevisionsOperationContinuationParameters
                    {
                        StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                        EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                        NodeTags = new Dictionary<string, string>(firstResult.NodeTags)
                    }
                };
                op = await store.Operations.SendAsync(new EnforceRevisionsConfigurationOperation(resumeParams));
                var resumeResult = (EnforceConfigurationResult)await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                // No additional trimming since everything was already enforced
                Assert.Equal(0, resumeResult.RemovedRevisions);
            }
        }

        [RavenTheory(RavenTestCategory.Revisions | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public async Task EnforceRevisionConfiguration_NodeTagValidation_Sharded_RejectsWrongNode(Options options)
        {
            // Sharded: verify that wrong NodeTags cause server-side rejection per shard
            using (var store = GetDocumentStore(options))
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionsToKeep = 10 }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                for (int i = 0; i < 5; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Company { Name = $"c1-v{i}" }, "companies/1");
                        await session.SaveChangesAsync();
                    }
                }

                configuration.Default.MinimumRevisionsToKeep = 2;
                await RevisionsHelper.SetupRevisionsAsync(store, configuration: configuration);

                // First run
                var op = await store.Operations.SendAsync(
                    new EnforceRevisionsConfigurationOperation(new EnforceRevisionsConfigurationOperation.Parameters { IncludeForceCreated = false }));
                var firstResult = (EnforceConfigurationResult)await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));

                // Tamper with NodeTags — replace all shard node tags with a fake node
                var fakeNodeTags = new Dictionary<string, string>();
                foreach (var kvp in firstResult.NodeTags)
                    fakeNodeTags[kvp.Key] = "Z";

                var resumeParams = new EnforceRevisionsConfigurationOperation.Parameters
                {
                    IncludeForceCreated = false,
                    ContinuationParameters = new RevisionsOperationContinuationParameters
                    {
                        StartFromEtags = new Dictionary<string, long>(firstResult.LastProcessedEtags),
                        EtagBarriers = new Dictionary<string, long>(firstResult.EtagBarriersUsed),
                        NodeTags = fakeNodeTags
                    }
                };

                // The sharded orchestrator forwards the params to each shard; at least one shard should reject
                op = await store.Operations.SendAsync(new EnforceRevisionsConfigurationOperation(resumeParams));
                var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
                {
                    await op.WaitForCompletionAsync(TimeSpan.FromSeconds(30));
                });

                Assert.Contains("node 'Z'", ex.Message);
            }
        }
    }
}
