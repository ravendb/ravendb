using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.FileSystem;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Common;
using Raven.Tests.Common.Util;
using Xunit;

namespace RavenFS.Tests.Smuggler
{
    public class SmugglerExecutionTests : RavenFilesTestWithLogs
    {
        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldThrowIfFileSystemDoesNotExist()
        {
            var path = Path.GetTempFileName();

            using (var store = NewStore())
            {
                var smugglerApi = new SmugglerFilesApi();

                var options = new FilesConnectionStringOptions
                {
                    Url = store.Url,
                    DefaultFileSystem = "DoesNotExist"
                };

                var message = string.Format("Smuggler does not support database creation (database 'DoesNotExist' on server '{0}' must exist before running Smuggler).", store.Url);

                var e = await AssertAsync.Throws<SmugglerException>(() => smugglerApi.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = path, To = options }));
                Assert.Equal(message, e.Message);

                e = await AssertAsync.Throws<SmugglerException>(() => smugglerApi.ExportData(new SmugglerExportOptions<FilesConnectionStringOptions> { ToFile = path, From = options }));
                Assert.Equal(message, e.Message);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldNotThrowIfFileSystemExists()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewStore())
                {
                    await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync("DoesExist");
                    await InitializeWithRandomFiles(store, 1, 4);

                    var options = new FilesConnectionStringOptions { Url = store.Url, DefaultFileSystem = "DoesExist" };

                    var smugglerApi = new SmugglerFilesApi();
                    await smugglerApi.ExportData(new SmugglerExportOptions<FilesConnectionStringOptions> { From = options, ToFile = path });
                    await smugglerApi.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = path, To = options });                    
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task ShouldNotThrowIfFileSystemExistsUsingDefaultConfiguration()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewStore())
                {                    
                    await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(store.DefaultFileSystem);
                    await InitializeWithRandomFiles(store, 1, 4);

                    var options = new FilesConnectionStringOptions { Url = store.Url, DefaultFileSystem = store.DefaultFileSystem };

                    var smugglerApi = new SmugglerFilesApi();
                    await smugglerApi.ExportData(new SmugglerExportOptions<FilesConnectionStringOptions> { From = options, ToFile = path });
                    await smugglerApi.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = path, To = options });
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BehaviorWhenServerIsDown()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewStore())
                {
                    var options = new FilesConnectionStringOptions { Url = "http://localhost:8078/", DefaultFileSystem = store.DefaultFileSystem };

                    var smugglerApi = new SmugglerFilesApi();

                    var e = await AssertAsync.Throws<SmugglerException>(() => smugglerApi.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = path, To = options }));
                    Assert.Contains("Smuggler encountered a connection problem:", e.Message);

                    e = await AssertAsync.Throws<SmugglerException>(() => smugglerApi.ExportData(new SmugglerExportOptions<FilesConnectionStringOptions> { ToFile = path, From = options }));
                    Assert.Contains("Smuggler encountered a connection problem:", e.Message);
                }
            }
            finally
            {
                IOExtensions.DeleteFile(path);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void MaxChunkSizeInMbShouldBeRespectedByDataDumper()
        {
            throw new NotImplementedException();
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanDumpEmptyFileSystem()
        {
            var backupPath = NewDataPath("BackupFolder");

            try
            {
                using (var store = (FilesStore)NewStore())
                {
                    // now perform full backup
                    var dumper = new SmugglerFilesApi { Options = { Incremental = true } };

                    await dumper.ExportData(
                        new SmugglerExportOptions<FilesConnectionStringOptions>
                        {
                            ToFile = backupPath,
                            From = new FilesConnectionStringOptions
                            {
                                Url = "http://localhost:8079",
                                DefaultFileSystem = store.DefaultFileSystem,
                            }
                        });
                }

                VerifyDump(backupPath, store => { throw new NotImplementedException(); });
            }
            finally
            {
                IOExtensions.DeleteDirectory(backupPath);
            }  
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task CanHandleFilesExceptionsGracefully()
        {
            var backupPath = NewDataPath("BackupFolder");

            var alreadyReset = false;

            var forwarder = new ProxyServer(8070, 8079)
            {
                VetoTransfer = (totalRead, buffer) =>
                {
                    if (alreadyReset == false && totalRead > 25000)
                    {
                        alreadyReset = true;
                        return true;
                    }
                    return false;
                }
            };

            try
            {
                using (var store = NewStore())
                {
                    await InitializeWithRandomFiles(store, 10, 64);

                    // now perform full backup
                    var dumper = new SmugglerFilesApi { Options = { Incremental = true } };

                    ExportFilesResult exportResult = null;
                    try
                    {
                        exportResult = await dumper.ExportData(
                            new SmugglerExportOptions<FilesConnectionStringOptions>
                            {
                                ToFile = backupPath,
                                From = new FilesConnectionStringOptions
                                {
                                    Url = "http://localhost:8079",
                                    DefaultFileSystem = store.DefaultFileSystem,
                                }
                            });
                    }
                    catch (AggregateException e)
                    {
                        var inner = (SmugglerExportException)e.ExtractSingleInnerException();
                        exportResult = new ExportFilesResult
                        {
                            FilePath = inner.File
                        };
                    }

                    Assert.NotNull(exportResult);
                    Assert.True(!string.IsNullOrWhiteSpace(exportResult.FilePath));

                    exportResult = await dumper.ExportData(
                                        new SmugglerExportOptions<FilesConnectionStringOptions>
                                        {
                                            ToFile = backupPath,
                                            From = new FilesConnectionStringOptions
                                            {
                                                Url = "http://localhost:8079",
                                                DefaultFileSystem = store.DefaultFileSystem,
                                            }
                                        });

                    using (var fileStream = new FileStream(exportResult.FilePath, FileMode.Open))
                    using (var stream = new GZipStream(fileStream, CompressionMode.Decompress))
                    {
                        throw new NotImplementedException();
                    }

                }
            }
            finally
            {
                forwarder.Dispose();
                IOExtensions.DeleteDirectory(backupPath);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task MetadataIsPreserved()
        {
            var backupPath = NewDataPath("BackupFolder");
            try
            {
                var dumper = new SmugglerFilesApi { Options = { Incremental = true } };

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


        private async Task InitializeUniformFile(IFilesStore store, string name, int size, char content)
        {
            using (var session = store.OpenAsyncSession())
            {
                session.RegisterUpload( name, CreateUniformFileStream(size, content));
                await session.SaveChangesAsync();
            }
        }

        private async Task InitializeWithRandomFiles(IFilesStore store, int count, int maxFileSizeInKb = 1024)
        {
            var rnd = new Random();

            var creationTasks = new Task[count];
            for (int i = 0; i < count; i++)
            {               
                string name = "file-" + rnd.Next() + ".bin";
                int size = rnd.Next(maxFileSizeInKb) * 1024;
                var content = (char) rnd.Next(byte.MaxValue);

                creationTasks[i] = InitializeUniformFile(store, name, size, content);
            }

            await Task.WhenAll(creationTasks);
        }

        private void VerifyDump(string backupPath, Action<FilesStore> action)
        {
            using (var store = NewStore())
            {
                var dumper = new SmugglerFilesApi { Options = { Incremental = true } };
                dumper.ImportData(new SmugglerImportOptions<FilesConnectionStringOptions> { FromFile = backupPath }).Wait();

                action(store);
            }
        }
    }
}
