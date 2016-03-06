using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Database.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4189 : RavenFilesTestBase
    {
        private readonly string backupLocation =
            Path.Combine(AppDomain.CurrentDomain.BaseDirectory, Guid.NewGuid().ToString());

        [Fact]
        public async Task Non_existing_index_storage_folder_should_throw_proper_exception()
        {
            using (var store = NewStore(requestedStorage: "voron"))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test.file", new MemoryStream(new byte[] { 1, 2, 3 }));
                    await session.SaveChangesAsync();
                }

                var opId = await store.AsyncFilesCommands.Admin.StartBackup(backupLocation,
                    null, false, store.DefaultFileSystem);
                await WaitForOperationAsync(store.Url, opId);

                FileSystemDocument document;
                using (var file = File.OpenText(Path.Combine(backupLocation, Constants.FilesystemDocumentFilename)))
                using (var reader = new JsonTextReader(file))
                    document = (new JsonSerializer()).Deserialize<FileSystemDocument>(reader);

                var drives = DriveInfo.GetDrives().Select(x => x.Name.ToLower()[0]).ToArray();
                var lastDriveLetter = 'a';
                while (lastDriveLetter != 'z')
                {
                    if (drives.Contains(lastDriveLetter) == false)
                        break;
                    lastDriveLetter++;
                }

                document.Settings[Constants.FileSystem.IndexStorageDirectory] = string.Format("{0}:\\", lastDriveLetter); //on purpose, non existing path

                using (var file = File.CreateText(Path.Combine(backupLocation, Constants.FilesystemDocumentFilename)))
                using (var writer = new JsonTextWriter(file))
                    (new JsonSerializer()).Serialize(writer, document);
                
                Assert.Throws<BadRequestException>(() => 
                    AsyncHelpers.RunSync(() => store.AsyncFilesCommands.Admin.StartRestore(new FilesystemRestoreRequest
                    {
                        BackupLocation = backupLocation
                    })));
            }
        }

        [Fact]
        public async Task Non_existing_data_folder_should_throw_proper_exception()
        {
            using (var store = NewStore(requestedStorage: "voron", runInMemory: true))
            {
                using (var session = store.OpenAsyncSession())
                {
                    session.RegisterUpload("test.file", new MemoryStream(new byte[] { 1, 2, 3 }));
                    await session.SaveChangesAsync();
                }

                var opId = await store.AsyncFilesCommands.Admin.StartBackup(backupLocation,
                    null, false, store.DefaultFileSystem);
                await WaitForOperationAsync(store.Url, opId);

                FileSystemDocument document;
                using (var file = File.OpenText(Path.Combine(backupLocation, Constants.FilesystemDocumentFilename)))
                using (var reader = new JsonTextReader(file))
                    document = (new JsonSerializer()).Deserialize<FileSystemDocument>(reader);

                var drives = DriveInfo.GetDrives().Select(x => x.Name.ToLower()[0]).ToArray();
                var lastDriveLetter = 'a';
                while (lastDriveLetter != 'z')
                {
                    if (drives.Contains(lastDriveLetter) == false)
                        break;
                    lastDriveLetter++;
                }

                document.Settings[Constants.FileSystem.DataDirectory] = string.Format("{0}:\\", (char)(lastDriveLetter )); //on purpose, non existing path

                using (var file = File.CreateText(Path.Combine(backupLocation, Constants.FilesystemDocumentFilename)))
                using (var writer = new JsonTextWriter(file))
                    (new JsonSerializer()).Serialize(writer, document);

                Assert.Throws<BadRequestException>(() =>
                    AsyncHelpers.RunSync(() => store.AsyncFilesCommands.Admin.StartRestore(new FilesystemRestoreRequest
                    {
                        BackupLocation = backupLocation
                    })));
            }
        }


        public override void Dispose()
        {
            base.Dispose();
            IOExtensions.DeleteDirectory(backupLocation);
        }
    }
}
