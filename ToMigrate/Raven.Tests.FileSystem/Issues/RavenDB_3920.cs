// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3920.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Database.FileSystem.Extensions;
using Raven.Database.FileSystem.Storage;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_3920 : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task cleanup_of_deleted_files_wont_delete_existing_file_if_its_previous_upload_failed_and_broken_file_was_indicated_to_delete()
        {
            const string fileName = "file.bin";

            using (var store = NewStore())
            {
                var rfs = GetFileSystem();

                await store.AsyncFilesCommands.UploadAsync(fileName, new MemoryStream(new byte[2 * 1024 * 1024]));

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload(fileName, 10, s =>
                    {
                        s.WriteByte(1);
                        s.WriteByte(2);
                        s.WriteByte(3);
                    });

                    await ThrowsAsync<ErrorResponseException>(() => session.SaveChangesAsync()); // 10 bytes declared but only 3 has been uploaded, IndicateFileToDelete is going to be called underhood
                }

                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload(fileName, 3, s =>
                    {
                        s.WriteByte(1);
                        s.WriteByte(2);
                        s.WriteByte(3);
                    });

                    await session.SaveChangesAsync();
                }

                await rfs.Files.CleanupDeletedFilesAsync(); // should not delete existing file

                var downloaded = new MemoryStream();

                (await store.AsyncFilesCommands.DownloadAsync(fileName)).CopyTo(downloaded);

                Assert.Equal(3, downloaded.Length);
            }
        }

        [Fact]
        public async Task resumed_rename_operation_needs_to_take_into_account_file_etag_to_avoid_renaming_next_version_of_file()
        {
            string name = FileHeader.Canonize("file.bin");
            string renamed = FileHeader.Canonize("renamed.bin");

            using (var store = NewStore())
            {
                var rfs = GetFileSystem();

                await store.AsyncFilesCommands.UploadAsync(name, new MemoryStream(), new RavenJObject { { "version", 1 } });

                // instead of this:
                // await store.AsyncFilesCommands.RenameAsync(fileName, newName);
                // let's create a config to indicate rename operation - for example restart in the middle could happen
                var renameOpConfig = RavenFileNameHelper.RenameOperationConfigNameForFile(name);
                var renameOperation = new RenameFileOperation(name, renamed, (await store.AsyncFilesCommands.GetAsync(new [] {name}))[0].Etag, new RavenJObject {{"version", 1}});

                rfs.Storage.Batch(accessor => accessor.SetConfigurationValue(renameOpConfig, renameOperation));

                // upload new file under the same name, before ResumeFileRenamingAsync is called
                await store.AsyncFilesCommands.UploadAsync(name, new MemoryStream(), new RavenJObject { { "version", 2 } });

                await rfs.Files.ResumeFileRenamingAsync();

                var version2 = await store.AsyncFilesCommands.GetMetadataForAsync(name);
                Assert.NotNull(version2);
                Assert.Equal(2, version2["version"]);

                Assert.DoesNotContain(RavenFileNameHelper.RenameOperationConfigNameForFile(renameOperation.Name), await store.AsyncFilesCommands.Configuration.GetKeyNamesAsync());
            }
        }
    }
}
