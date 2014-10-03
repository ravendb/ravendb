using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Client.FileSystem;
using Raven.Database.Extensions;
using Raven.Database.Server.RavenFS;
using Raven.Database.Smuggler;
using Raven.Json.Linq;
using Xunit;

namespace RavenFS.Tests.Smuggler
{
    public class DataDumperTests : RavenFilesTestWithLogs
    {

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpEmptyFileSystem()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {                
                using (var server = NewStore())
                {
                    using (new FilesStore { Url = "http://localhost:8079" }.Initialize())
                    {
                        // now perform full backup
                        var dumper = new FilesDataDumper(null as RavenFileSystem) { Options = { Incremental = true } };
                        await dumper.ExportData(new SmugglerExportOptions<FilesConnectionStringOptions> { ToFile = backupPath });
                    }
                }

                VerifyDump(backupPath, store => { throw new NotImplementedException(); });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            } 
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task MetadataIsPreserved()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                FileHeader originalFile;
                using (var server = NewStore())
                {
                    using (var session = server.OpenAsyncSession())
                    {
                        session.RegisterUpload("test1.file", CreateUniformFileStream(128));
                        await session.SaveChangesAsync();

                        // content update after a metadata change
                        originalFile = await session.LoadFileAsync("test1.file");
                        originalFile.Metadata["Test"] = new RavenJValue("Value");

                        await session.SaveChangesAsync();
                    }

                    using (new FilesStore { Url = "http://localhost:8079" }.Initialize())
                    {
                        // now perform full backup
                        var dumper = new FilesDataDumper(null as RavenFileSystem) { Options = { Incremental = true } };
                        await dumper.ExportData(new SmugglerExportOptions<FilesConnectionStringOptions> { ToFile = backupPath });
                    }
                }

                VerifyDump(backupPath, store =>
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var file = session.LoadFileAsync("test1.file").Result;                        
                        Assert.Equal(originalFile.CreationDate, file.CreationDate);
                        Assert.Equal(originalFile.Directory, file.Directory);
                        Assert.Equal(originalFile.Extension, file.Extension);
                        Assert.Equal(originalFile.FullPath, file.FullPath);
                        Assert.Equal(originalFile.LastModified, file.LastModified);
                        Assert.Equal(originalFile.Name, file.Name);
                        Assert.Equal(originalFile.TotalSize, file.TotalSize);
                        Assert.Equal(originalFile.UploadedSize, file.UploadedSize);
                        Assert.True(file.Metadata.ContainsKey("Test"));
                    }
                });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        private void VerifyDump(string backupPath, Action<FilesStore> action)
        {
            using (var store = NewStore())
            {
                var dumper = new FilesDataDumper(null as RavenFileSystem) { Options = { Incremental = true } };
                dumper.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = backupPath }).Wait();

                action(store);
            }
        }

    }
}
