using System;
using System.IO;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Smuggler;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12204 : RavenTestBase
    {
        [Fact]
        public async Task CanMigrateFromRavenDb()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), "export.ravendbdump");
            var id = "users/1";

            using (var store1 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store1.Database);
                using (var session = store1.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor"
                    }, id);
                    {
                        session.CountersFor(id).Increment("Downloads", int.MaxValue);
                        session.CountersFor(id).Increment("ShouldBePositiveValueAfterSmuggler", long.MaxValue);
                        session.CountersFor(id).Increment("LittleCounter", 500);
                    }
                    await session.SaveChangesAsync();
                }
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store1.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Egor " + i;
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store1.OpenAsyncSession())
                {
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(12, revisionsMetadata.Count);
                }
                var operation = await store1.Smuggler.ExportAsync(new DatabaseSmugglerExportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
            }
            // all data import
            using (var store2 = GetDocumentStore())
            {
                var operation = await store2.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                using (var session = store2.OpenAsyncSession())
                {
                    var metadata = session.Advanced.GetMetadataFor(await session.LoadAsync<User>(id));
                    Assert.Equal("HasRevisions, HasCounters", metadata.GetString("@flags"));
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(13, revisionsMetadata.Count);  // +1 revision added when importing
                    var dic = await session.CountersFor(id).GetAllAsync();
                    Assert.Equal(3, dic.Count);
                    Assert.Equal(int.MaxValue, dic["Downloads"]);
                    Assert.Equal(long.MaxValue, dic["ShouldBePositiveValueAfterSmuggler"]);
                    Assert.Equal(500, dic["LittleCounter"]);
                }
            }

            // no DatabaseRecord, no RevisionDocuments, no Counters
            using (var store3 = GetDocumentStore())
            {
                var importOptions = new DatabaseSmugglerImportOptions();
                importOptions.OperateOnTypes -= DatabaseItemType.DatabaseRecord;
                importOptions.OperateOnTypes -= DatabaseItemType.RevisionDocuments;
                importOptions.OperateOnTypes -= DatabaseItemType.Counters;

                var operation = await store3.Smuggler.ImportAsync(importOptions, file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));
                using (var session = store3.OpenAsyncSession())
                {
                    var metadata = session.Advanced.GetMetadataFor(await session.LoadAsync<User>(id));
                    Assert.False(metadata.ContainsKey("@flags"));
                    Assert.False(metadata.ContainsKey("@counters"));
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(0, revisionsMetadata.Count);  // +1 revision added when importing
                    var dic = await session.CountersFor(id).GetAllAsync();
                    Assert.Equal(0, dic.Count);
                }
            }

            // if doc has counters AND revisions => they must be kept.
            using (var store4 = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store4.Database);
                using (var session = store4.OpenAsyncSession())
                {
                    await session.StoreAsync(new User
                    {
                        Name = "Egor"
                    }, id);
                    {
                        session.CountersFor(id).Increment("ShouldBeKeptAfterSmugglerImport", 322);
                    }
                    await session.SaveChangesAsync();
                }
                var importOptions = new DatabaseSmugglerImportOptions();
                importOptions.OperateOnTypes -= DatabaseItemType.DatabaseRecord;
                importOptions.OperateOnTypes -= DatabaseItemType.RevisionDocuments;
                importOptions.OperateOnTypes -= DatabaseItemType.Counters;

                var operation = await store4.Smuggler.ImportAsync(importOptions, file);
                await operation.WaitForCompletionAsync(TimeSpan.FromMinutes(1));

                using (var session = store4.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>(id);
                    Assert.Equal("Egor 9", user.Name);   // check if document changed.
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.Equal("HasRevisions, HasCounters", metadata.GetString("@flags"));
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(3, revisionsMetadata.Count);  // +1 revision added when importing
                    var dic = await session.CountersFor(id).GetAllAsync();
                    Assert.Equal(1, dic.Count);
                    Assert.Equal(322, dic["ShouldBeKeptAfterSmugglerImport"]);
                }
            }
        }
    }
}
