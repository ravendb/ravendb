using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using FastTests.Utils;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.ServerWide;
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

        private async Task EnforceConfiguration(DocumentStore store)
        {
            var db = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                await db.DocumentsStorage.RevisionsStorage.EnforceConfigurationIncludeForceCreated(_ => { }, token);
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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration1);

            await TriggerRevisionsDelete(type, store, "Docs/1");

            // WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                if(type==ChangingType.EnforceConfiguration)
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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration1);

            await TriggerRevisionsDelete(type, store, "Docs/1");

            // WaitForUserToContinueTheTest(store);

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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration1);

            using (var session = store.OpenAsyncSession())
            {
                session.Delete("Docs/1");
                await session.SaveChangesAsync();

                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(9, doc1RevCount); // 9 (10 - 2 upon update + 1 delete)
                                               // - When it will be shrink to 3? never, because you probably wont touch this doc again
                                               // So it should not take into account the 'UponUpdate'.
            }

            //--
            //Enforce
            await EnforceConfiguration(store);

            //3 revisions
            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(3, doc1RevCount); // Old, New, Delete
            }
        }
        

        [Fact] // ____Fixed - PurgeOnDelete doesnt work with Enforce Config on deleted doc
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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration);

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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration: configuration1);

            await EnforceConfiguration(store);

            // WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(0, doc1RevCount); // got 3
            }
        }


        //---------------------------------------------------------------------------------------------------------------------------------------

        //-Fixed
        [Fact] //
        public async Task OnlyConflictConfig_EnforceConfig_ShouldntDeletesAllRevisions()
        {
            using var src = GetDocumentStore();
            using var dst = GetDocumentStore();

            var dstConfig = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 2
            };
            await RevisionsHelper.SetupConflictedRevisions(dst, Server.ServerStore, configuration: dstConfig);

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

            // WaitForUserToContinueTheTest(dst);

            await EnforceConfiguration(dst);

            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(2, doc1RevCount); // got 0
            }
        }

        //---------------------------------------------------------------------------------------------------------------------------------------
        
        // Force-Created flag and checkbox

        //---------------------------------------------------------------------------------------------------------------------------------------

        [Fact] ///// regular\manual (non-conflicted) revisions shouldnt obey the conflicted-config!!!
        public async Task ForceCreatedRevisions_ShouldntObeyToConflictedRevisions()
        {
            using var src = GetDocumentStore();
            using var dst = GetDocumentStore();

            var dstConfig = new RevisionsCollectionConfiguration
            {
                Disabled = false,
                MinimumRevisionsToKeep = 4
            };
            await RevisionsHelper.SetupConflictedRevisions(dst, Server.ServerStore, configuration: dstConfig);

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

            // Console.WriteLine("Before Conflict");
            // using (var session = dst.OpenAsyncSession())
            // {
            //     var doc1Revisions = await session.Advanced.Revisions.GetForAsync<User>("Docs/1");
            //     var doc1Metadatas = await session.Advanced.Revisions.GetMetadataForAsync("Docs/1");
            //     for (int i = 0; i < doc1Metadatas.Count; i++)
            //     {
            //         Console.WriteLine(doc1Revisions[i].Name+" " + doc1Metadatas[i].GetString("@change-vector") + " " + doc1Metadatas[i].GetString("@flags"));
            //     }
            // }

            // WaitForUserToContinueTheTest(dst);


            await SetupReplicationAsync(src, dst); // Conflicts resolved
            await EnsureReplicatingAsync(src, dst);

            WaitForUserToContinueTheTest(dst);

            // Console.WriteLine("\nAfter Conflict");
            // using (var session = dst.OpenAsyncSession())
            // {
            //     var doc1Revisions = await session.Advanced.Revisions.GetForAsync<User>("Docs/1");
            //     var doc1Metadatas = await session.Advanced.Revisions.GetMetadataForAsync("Docs/1");
            //     for (int i = 0; i < doc1Metadatas.Count; i++)
            //     {
            //         Console.WriteLine(doc1Revisions[i].Name + " " + doc1Metadatas[i].GetString("@change-vector") + " " + doc1Metadatas[i].GetString("@flags"));
            //     }
            // }

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

        //  Fix - Talk with Karmel (add time window like in revert revisions)
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
            await RevisionsHelper.SetupRevisions(Server.ServerStore, dst.Database, configuration: configuration);


            Console.WriteLine(DateTime.UtcNow.AddDays(-1));

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

            WaitForUserToContinueTheTest(dst);


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

            // var dstConfig = new RevisionsCollectionConfiguration { Disabled = false, MinimumRevisionAgeToKeep = TimeSpan.FromHours(1) };
            // await RevisionsHelper.SetupConflictedRevisions(dst, Server.ServerStore, configuration: dstConfig);
            var configuration2 = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                    MinimumRevisionAgeToKeep = TimeSpan.FromHours(1)
                }
            };
            await RevisionsHelper.SetupRevisions(Server.ServerStore, dst.Database, configuration: configuration2);

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
                Assert.Equal(3, doc1RevCount); // OLD (DELETED) , NEW, OLD, OLD
            }

            dbDst.Time.UtcDateTime = () => DateTime.UtcNow.AddHours(2);
            await TriggerRevisionsDelete(ChangingType.EnforceConfiguration, dst);
            using (var session = dst.OpenAsyncSession())
            {
                var doc1RevCount = await session.Advanced.Revisions.GetCountForAsync("Docs/1");
                Assert.Equal(0, doc1RevCount); // OLDER (DELETED EARLIER) , OLD (DELETED), OLDER (DELETED), OLDER (DELETED)
            }

        }

    }
}
