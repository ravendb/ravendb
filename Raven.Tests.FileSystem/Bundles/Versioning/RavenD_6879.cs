// -----------------------------------------------------------------------
//  <copyright file="RavenD_6879.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Client.FileSystem.Bundles.Versioning;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Bundles.Versioning;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Bundles.Versioning
{
    public class RavenD_6879 : RavenFilesTestBase
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task Can_upload_file_which_doesnt_exist_but_there_are_its_historical_revisions(string requestedStorage)
        {
            using (var store = NewStore(activeBundles: "Versioning", requestedStorage: requestedStorage))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 5 });

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("file.bin", CreateRandomFileStream(10));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterFileDeletion("file.bin");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("file.bin", CreateRandomFileStream(10));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionsForAsync("file.bin", 0, 10);
                    Assert.Equal(2, revisions.Length);
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task Can_upload_file_which_doesnt_exist_but_there_is_more_than_10_historical_revisions(string requestedStorage)
        {
            using (var store = NewStore(activeBundles: "Versioning", requestedStorage: requestedStorage))
            {
                await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 20 });

                for (int i = 0; i < 15; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        session.RegisterUpload("file.bin", CreateRandomFileStream(10));
                        session.RegisterUpload("/dir/file.bin", CreateRandomFileStream(10));

                        await session.SaveChangesAsync();
                    }
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterFileDeletion("file.bin");
                    session.RegisterFileDeletion("/dir/file.bin");

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("file.bin", CreateRandomFileStream(10));
                    session.RegisterUpload("/dir/file.bin", CreateRandomFileStream(10));

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.GetRevisionsForAsync("file.bin", 0, int.MaxValue);
                    Assert.Equal(16, revisions.Length);

                    revisions = await session.GetRevisionsForAsync("/dir/file.bin", 0, int.MaxValue);
                    Assert.Equal(16, revisions.Length);
                }
            }
        }
    }
}