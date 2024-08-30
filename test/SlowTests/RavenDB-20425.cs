using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.Revisions;
using Raven.Server.ServerWide;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests
{
    public class RavenDB_20425 : ReplicationTestBase
    {
        public RavenDB_20425(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public enum ChangingType
        {
            EnforceConfiguration,
            UpdateDocument
        }

        private Task TriggerRevisionsDelete(ChangingType type, DocumentStore store, string docId = null)
        {

            if (type == ChangingType.UpdateDocument)
            {
                if (docId == null)
                    throw new InvalidOperationException("docId cannot be null while using 'UpdateDocument' type.");

                return UpdateDoc(store, docId);
            }

            if (type == ChangingType.EnforceConfiguration)
            {
                return EnforceConfiguration(store);
            }

            return Task.FromException(new InvalidOperationException($"Update type: {type} isn't handled"));
        }

        private async Task EnforceConfiguration(DocumentStore store, bool includeForceCreated = true)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, includeForceCreated, token: token);
        }

        private async Task UpdateDoc(DocumentStore store, string docId)
        {
            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>(docId);
                user.Name += "1";
                await session.SaveChangesAsync();
            }
        }


        //---------------------------------------------------------------------------------------------------------------------------------------

        // Right Behavior
        [Theory]
        [InlineData(ChangingType.EnforceConfiguration)] // Works
        [InlineData(ChangingType.UpdateDocument)] // Fails
        public async Task RemoveDefaultConfig_ThenChangingDoc_ShouldDeleteRevisions(ChangingType type)
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
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            // Create a doc with 2 revisions
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

            // Remove all configurations except the Conflicts Config
            var configuration1 = new RevisionsConfiguration
            {
                Default = null
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration1);


            await TriggerRevisionsDelete(type, store, "Docs/1");

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                if (type == ChangingType.EnforceConfiguration)
                    Assert.Equal(0, doc1RevCount); // EnforceConfig: 0
                if (type == ChangingType.UpdateDocument)
                    Assert.Equal(2, doc1RevCount); // UpdateDocument: 2 (user should use "enforce config" for delete the redundent revisions)
            }
        }

        // Right Behavior
        [Theory]
        [InlineData(ChangingType.EnforceConfiguration)] // Fails
        [InlineData(ChangingType.UpdateDocument)] // Fails
        public async Task DisableCollectionAutoCreationConfig_ThenChangingDoc_ShouldObeyCollectionConfig(ChangingType type)
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 2
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = false,
                        MinimumRevisionsToKeep = 3
                    }
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            // Create doc with 3 revisions
            for (int i = 0; i < 3; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"New{i}" }, "Docs/1");
                    await session.SaveChangesAsync();
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);
            }

            // disable "Users" collection config
            var configuration1 = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 2
                },
                Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                {
                    ["Users"] = new RevisionsCollectionConfiguration
                    {
                        Disabled = true, //!!!
                        MinimumRevisionsToKeep = 3
                    }
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration1);

            await TriggerRevisionsDelete(type, store, "Docs/1");

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);
            }
        }


        //---------------------------------------------------------------------------------------------------------------------------------------

        [Fact]
        public async Task ConfigurationWithMin10_DeleteDocWith10_changeConfigToMin3AndUponUpdate2_DeletedDocShouldRemainWith3Revisions()
        {
            using var store = GetDocumentStore();

            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 10
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            // Create a doc with 10 revisions
            for (int i = 0; i < 10; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"New{i}" }, "Docs/1");
                    await session.SaveChangesAsync();
                }
            }

            var configuration1 = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 3,
                    MaximumRevisionsToDeleteUponDocumentUpdate = 2
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration1);

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Docs/1");
                await session.SaveChangesAsync();

                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(9, doc1RevCount); // 9 (10 - 2 upon update + 1 delete)
                                               // - When it will be shrink to 3? never, because you probably wont touch this doc again
                                               // So it should not take into account the 'UponUpdate'.
            }

            //Enforce
            await EnforceConfiguration(store);

            //3 revisions
            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount); // Old, New, Delete
            }
        }


        [Fact]
        public async Task DeleteDocWithRevisions_ThenAddPurgeOnDeleteConfig_EnforceConfig_ShouldDeleteTheRevisionsOfTheDeletedDoc()
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
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            // Create a doc with 2 revisions
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

            // Delete the doc
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Docs/1");
                await session.SaveChangesAsync();

                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount); // Old, New, Delete
            }

            var configuration1 = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 3,
                    PurgeOnDelete = true,
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration1);

            await EnforceConfiguration(store);

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(0, doc1RevCount); // got 3
            }
        }


        //---------------------------------------------------------------------------------------------------------------------------------------

        // (RavenDB-19641)
        [Fact]
        public async Task OnlyConflictConfig_EnforceConfig_ShouldntDeletesAllRevisions()
        {
            using var src = GetDocumentStore();
            using var dst = GetDocumentStore();

            var dstConfig = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 2
            };
            await RevisionsHelper.SetupConflictedRevisionsAsync(dst, Server.ServerStore, configuration: dstConfig);

            // Create a doc with 2 'conflicted' (or 'resolved') revisions
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            using (var session = dst.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            await SetupReplicationAsync(src, dst); // Conflicts resolved
            await EnsureReplicatingAsync(src, dst);
            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount); // obeys the Conflicted Config
            }

            await EnforceConfiguration(dst);

            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount); // got 0
            }
        }

        //---------------------------------------------------------------------------------------------------------------------------------------

        // regular\manual (non-conflicted) revisions shouldnt obey the conflicted-config!
        [Fact]
        public async Task ForceCreatedRevisions_ShouldntObeyToConflictedRevisions()
        {
            using var src = GetDocumentStore();
            using var dst = GetDocumentStore();

            var dstConfig = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 4
            };
            await RevisionsHelper.SetupConflictedRevisionsAsync(dst, Server.ServerStore, configuration: dstConfig);

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Src" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            for (int i = 1; i <= 10; i++)
            {
                using (var session = dst.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"Dst{i}" }, "Docs/1");
                    await session.SaveChangesAsync();

                    session.Advanced.Revisions.ForceRevisionCreationFor("Docs/1");
                    await session.SaveChangesAsync();
                }
            }


            await SetupReplicationAsync(src, dst); // Conflicts resolved
            await EnsureReplicatingAsync(src, dst);

            // WaitForUserToContinueTheTest(dst);

            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(12, doc1RevCount); // GOT 12 INSTEAD OF 13,Because 1 Revision which is force created (in the future will be with force-created flag)
                                                // got a "Conflicted" to its flag
                                                // IN ANOTHER TEST: WHAT SHOULD WE DO WITH FORCE-CREATED WHICH IS ALSO CONFLICTED, WHEN ENFORCE TRYING
                                                //                  TO DELETE IT WHEN YOU HAVE ONLY CONFLICTED CONFIG AND YOU SHOULDNT DELETE FORCE-CREATED. ?
            }
        }

        //---------------------------------------------------------------------------------------------------------------------------------------

        [Fact]
        public async Task NotDeletingOutOfDateRevisionsBecauseTheyOrderedByEtag()
        {
            using var src = GetDocumentStore();
            using var dst = GetDocumentStore(
                new Options()
                {
                    ModifyDatabaseRecord = record =>
                    {
                        record.ConflictSolverConfig = new ConflictSolver
                        {
                            ResolveToLatest = false,
                            ResolveByCollection = new Dictionary<string, ScriptResolver>()
                            {
                            {
                                "Users", new ScriptResolver()
                                {
                                    Script = @"var oldestDoc = docs[0];
for (var i = 0; i < docs.length; i++) {
    var curDate = Date.parse(docs[i]['@metadata']['@last-modified']);
    var oldDate = Date.parse(oldestDoc['@metadata']['@last-modified']);
    if(curDate < oldDate){
        oldestDoc = docs[i];
    }
}
return oldestDoc;"
                                }
                            }
                            }
                        };
                    }
                });

            var dbSrc = await Databases.GetDocumentDatabaseInstanceFor(src);
            var dbDst = await Databases.GetDocumentDatabaseInstanceFor(dst);
            dbDst.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-1);
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(dst, Server.ServerStore, configuration: configuration);

            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Src" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = dst.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Dst1" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = dst.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Dst2" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            await SetupReplicationAsync(src, dst); // Conflicts resolved
            await EnsureReplicatingAsync(src, dst);


            // Ensure revisions arent ordered by "last modified".
            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(4, doc1RevCount);

                var times = (await session.Advanced.Revisions.GetMetadataForAsync("Docs/1"))
                    .Select(metadata => DateTime.Parse(metadata["@last-modified"].ToString()));

                DateTime previousTime = DateTime.MinValue;
                var orderedByTime = true;
                foreach (var currentTime in times)
                {
                    // Console.WriteLine(currentTime.ToString("dd-MM-yyyy HH:mm:ss.fffffff"));

                    if (currentTime < previousTime)
                    {
                        orderedByTime = false;
                        previousTime = currentTime;
                        break;
                    }

                    previousTime = currentTime;
                }
                Assert.False(orderedByTime);
            }

            var configuration2 = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionAgeToKeep = TimeSpan.FromHours(1)
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(dst, Server.ServerStore, configuration: configuration2);

            await TriggerRevisionsDelete(ChangingType.EnforceConfiguration, dst);
            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(4, doc1RevCount); // NEW, NEWER, NEW, NEW
            }

            dbDst.Time.UtcDateTime = () => DateTime.UtcNow;
            await TriggerRevisionsDelete(ChangingType.EnforceConfiguration, dst);
            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(1, doc1RevCount); // OLD (DELETED) , NEW, OLD (DELETED), OLD (DELETED)
            }

            dbDst.Time.UtcDateTime = () => DateTime.UtcNow.AddHours(2);
            await TriggerRevisionsDelete(ChangingType.EnforceConfiguration, dst);
            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(0, doc1RevCount); // OLDER (DELETED EARLIER) , OLD (DELETED), OLDER (DELETED EARLIER), OLDER (DELETED EARLIER)
            }

        }

        //---------------------------------------------------------------------------------------------------------------------------------------

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Exclude_ForceCreated_Revisions_On_EnforceConfig_InCaseOf_NoConfiguration(bool deleteAlsoForceCreated)
        {
            using var store = GetDocumentStore();

            var configuration5 = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 5
                }
            };
            var noConfiguration = new RevisionsConfiguration
            {
                Default = null
            };

            // Setup Config with MinimumRevisionsToKeep=5
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration5);
            // Create 5 regular revision
            for (int i = 1; i <= 5; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"Old{i}" }, "Docs/1");
                    await session.SaveChangesAsync();
                }
            }

            // Remove all configurations except the Conflicts Config
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: noConfiguration);
            // Create 10 force-created revisions
            for (int i = 1; i <= 10; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"FC{i}" }, "Docs/1");
                    await session.SaveChangesAsync();

                    session.Advanced.Revisions.ForceRevisionCreationFor("Docs/1");
                    await session.SaveChangesAsync();
                }
            }
            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(15, doc1RevCount);
            }

            // Enforce configuration
            await EnforceConfiguration(store, includeForceCreated: deleteAlsoForceCreated);
            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                if (deleteAlsoForceCreated)
                    Assert.Equal(0, doc1RevCount); // all revisions were deleted
                else
                    Assert.Equal(10, doc1RevCount);   // only forced-created remained
            }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Exclude_ForceCreated_Revisions_On_EnforceConfig_InCaseOf_NoConfiguration2(bool deleteAlsoForceCreated)
        {
            using var src = GetDocumentStore();
            using var dst = GetDocumentStore();

            // Create a doc with 3 'conflicted' (or 'resolved') revisions in 'dst'
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            using (var session = dst.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            await SetupReplicationAsync(src, dst); // Conflicts resolved
            await EnsureReplicatingAsync(src, dst);
            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount); // obeys the Conflicted Config
            }

            // Create 10 force-created revisions in dst
            for (int i = 1; i <= 10; i++)
            {
                using (var session = dst.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"FC{i}" }, "Docs/1");
                    await session.SaveChangesAsync();

                    session.Advanced.Revisions.ForceRevisionCreationFor("Docs/1");
                    await session.SaveChangesAsync();
                }
            }
            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(13, doc1RevCount);
            }

            var dstConfig = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 2,
                PurgeOnDelete = true
            };
            await RevisionsHelper.SetupConflictedRevisionsAsync(dst, Server.ServerStore, configuration: dstConfig);

            await EnforceConfiguration(dst, deleteAlsoForceCreated);
            WaitForUserToContinueTheTest(dst);

            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                if (deleteAlsoForceCreated)
                    Assert.Equal(2, doc1RevCount);
                else
                    Assert.Equal(12, doc1RevCount);
            }

        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task Include_ForceCreated_AlwaysOn_EnforceConfig_InCaseOf_PurgeOnDelete(bool deleteAlsoForceCreated)
        {
            using var src = GetDocumentStore();
            using var dst = GetDocumentStore();

            // Create a doc with 3 'conflicted' (or 'resolved') revisions in 'dst'
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            for (int i = 1; i <= 10; i++)
            {
                using (var session = dst.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"ForceCreated{i}" }, "Docs/1");
                    await session.SaveChangesAsync();

                    session.Advanced.Revisions.ForceRevisionCreationFor("Docs/1");
                    await session.SaveChangesAsync();
                }
            }
            using (var session = dst.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            await SetupReplicationAsync(src, dst); // Conflicts resolved
            await EnsureReplicatingAsync(src, dst);

            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(13, doc1RevCount);

                session.Delete("Docs/1");
                await session.SaveChangesAsync();

                doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(14, doc1RevCount); // 10 force-created, 3 conflict, 1 delete

                // check if delete revision has been created (even if you have no config - you have revisions so you need to create "delete revision" in delete).
                var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync("Docs/1");
                Assert.Equal(14, revisionsMetadata.Count);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));
            }

            var dstConfig = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 2,
                PurgeOnDelete = true
            };
            await RevisionsHelper.SetupConflictedRevisionsAsync(dst, Server.ServerStore, configuration: dstConfig);

            await EnforceConfiguration(dst, deleteAlsoForceCreated);

            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                if (deleteAlsoForceCreated)
                    Assert.Equal(0, doc1RevCount);
                else
                    Assert.Equal(11, doc1RevCount); // 10 ForceCreated, 1 Deleted
            }

        }


        //---------------------------------------------------------------------------------------------------------------------------------------

        // (RavenDB-19640)
        [Fact (Skip = "Not sure the logic here is correct")]
        public async Task ConfigureRevisionsForConflictsOperation_PurgeOnDelete_Doesnt_Work_As_Expected_On_Document_Delete()
        {
            using (var source = GetDocumentStore())
            using (var destination = GetDocumentStore())
            {
                using (var session = source.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Foo" }, "FoObAr/0");
                    await session.SaveChangesAsync();
                }

                using (var session = destination.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Foo2" }, "FoObAr/0");
                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(source, destination);
                await EnsureReplicatingAsync(source, destination);

                using (var session = destination.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetCountForAsync("FoObAr/0");
                    Assert.Equal(3, revisions);
                }

                await destination.Maintenance.Server.SendAsync(new ConfigureRevisionsForConflictsOperation(destination.Database, new RevisionsCollectionConfiguration
                {
                    PurgeOnDelete = true
                }));

                using (var session = destination.OpenAsyncSession())
                {
                    session.Delete("FoObAr/0");
                    await session.SaveChangesAsync();
                }

                // WaitForUserToContinueTheTest(destination, false);

                using (var session = destination.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetCountForAsync("FoObAr/0");
                    Assert.Equal(0, revisions);
                }
            }
        }

        // (RavenDB-20040)
        [Fact]
        public async Task EnforceRevisions_Shouldnt_Deletes_ConflictRevisions_When_Having_ConflictConfig()
        {
            using var src = GetDocumentStore();
            using var dst = GetDocumentStore();

            var dstConfig = new RevisionsCollectionConfiguration
            {
                MinimumRevisionsToKeep = 2
            };
            await RevisionsHelper.SetupConflictedRevisionsAsync(dst, Server.ServerStore, configuration: dstConfig);

            // Create a doc with 3 'conflicted' (or 'resolved') revisions in 'dst'
            using (var session = src.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            using (var session = dst.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            await SetupReplicationAsync(src, dst); // Conflicts resolved
            await EnsureReplicatingAsync(src, dst);

            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount); // obeys the Conflicted Config
            }

            await EnforceConfiguration(dst);
            // WaitForUserToContinueTheTest(dst);

            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount);
            }

        }


        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task DeleteAllRevisions_IncludeForceCreatedRevisions(bool includeForceCreated, bool checkUponUpdate)
        {
            using var store = GetDocumentStore();

            // Add config
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionsToKeep = 100
                }
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            // Create doc with 3 revisions
            for (int i = 0; i < 3; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"Regular{i}" }, "Docs/1");
                    await session.SaveChangesAsync();
                }
            }

            for (int i = 0; i < 5; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"Regular{i}" }, "Docs/2");
                    await session.SaveChangesAsync();
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);

                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(5, doc2RevCount);
            }

            // Remove config (remove all configs except conflict config).
            var noConfiguration = new RevisionsConfiguration
            {
                Default = null
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: noConfiguration);

            // Create doc with 2 force-created revisions
            for (int i = 3; i < 5; i++)
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = $"ForceCreated{i}" }, "Docs/1");
                    await session.SaveChangesAsync();

                    session.Advanced.Revisions.ForceRevisionCreationFor("Docs/1");
                    await session.SaveChangesAsync();
                }
            }
            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(5, doc1RevCount);

                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(5, doc2RevCount);
            }

            if (checkUponUpdate)
            {
                var maxUponUpdateConfig = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        MinimumRevisionsToKeep = 0,
                        MaximumRevisionsToDeleteUponDocumentUpdate = 1
                    }
                };
                await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: maxUponUpdateConfig);
            }

            await store.Maintenance.SendAsync(new DeleteRevisionsOperation(new List<string>() { "Docs/2", "Docs/1" } , removeForceCreatedRevisions: includeForceCreated));

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(includeForceCreated ? 0 : 2, doc1RevCount);

                var doc2RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/2");
                Assert.Equal(0, doc2RevCount);
            }

        }

        //-----------------------------------------------------------------------------------------------------------------------------------------

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DocWithRevisionsAndNoConfig_ShouldCreateDeleteRevisionInDelete(bool disableConfiguration)
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
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            // Create a doc with 2 revisions
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

            RevisionsConfiguration configuration1;
            if (disableConfiguration)
            {
                // Disable default configuration
                configuration1 = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration
                    {
                        Disabled = true,
                        MinimumRevisionsToKeep = 100
                    }
                };
            }
            else
            {
                // Remove all configurations except the Conflicts Config
                configuration1 = new RevisionsConfiguration
                {
                    Default = null
                };
            }
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration1);

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);

                var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync("Docs/1");
                Assert.Equal(3, revisionsMetadata.Count);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));
            }

            /*
                Preventing revisions to become orphans.
                The scenario we wanted to solve:
                    Go into the studio.
                    Create a Doc.
                    Create a force-created revision.
                    Delete the doc.
                    The doc wasn't in the 'Revisions Bin' and its revisions was still existing but we weren't able to see them (they are orphaned).
                
                this change is letting you see them in the revisions bin.
             */
        }

        [Fact]
        public async Task DocWithForceCreatedRevisionsAndNoConfig_ShouldCreateDeleteRevisionInDelete()
        {
            using var store = GetDocumentStore();

            // Create a doc with 2 revisions
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old" }, "Docs/1");
                await session.SaveChangesAsync();
                session.Advanced.Revisions.ForceRevisionCreationFor("Docs/1");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
                session.Advanced.Revisions.ForceRevisionCreationFor("Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);

                var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync("Docs/1");
                Assert.Equal(3, revisionsMetadata.Count);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));
            }
        }

        //-----------------------------------------------------------------------------------------------------------------------------------------

        [Fact]
        public async Task RevivedDocumentShouldHaveTheRevisionsOfTheDeletedDoc()
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
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: configuration);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old1" }, "Docs/1");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Old2" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            // Remove all configurations except the Conflicts Config
            var emptyConfig = new RevisionsConfiguration
            {
                Default = null
            };
            await RevisionsHelper.SetupRevisionsAsync(store, Server.ServerStore, configuration: emptyConfig);

            // Delete doc
            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Docs/1");
                await session.SaveChangesAsync();
            }

            // Assert doc has 'Delete Revision' although it has been deleted after we deleted the configuration
            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);

                var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync("Docs/1");
                Assert.Equal(3, revisionsMetadata.Count);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));
            }

            // Create the doc again and assert that it has 'HasRevisions' flag and the old doc revisions
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "New" }, "Docs/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var doc = await session.LoadAsync<User>("Docs/1");
                var metadata = session.Advanced.GetMetadataFor(doc);
                Assert.Contains(DocumentFlags.HasRevisions.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));

                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount);

                var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync("Docs/1");
                Assert.Equal(3, revisionsMetadata.Count);
                Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));
            }

            /*
             *The scenario I wanted to solve:
                Go into the studio.
                Create a Doc.
                Create a force-created revision.
                Delete the doc.
                create the doc again (a doc with the same id).
                The new (revived) doc will have 2 revisions, but you won't be able to see them (because it wont have a 'HasRevisions' flag).

                this change is letting you to see them in the revisions bin, even if their doc has no config.
             */
        }

    }
}
