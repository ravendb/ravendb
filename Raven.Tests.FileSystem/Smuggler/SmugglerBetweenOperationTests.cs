using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Smuggler;
using Raven.Tests.Common;
using Raven.Tests.Common.Util;
using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.FileSystem.Smuggler
{
    partial class SmugglerExecutionTests
    {
        private const string SourceFilesystem = "FS1";
        private const string DestinationFilesystem = "FS2";


        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_ShouldNotCreateFilesystem()
        {
            using (var store = NewStore())
            {
                var smugglerApi = new SmugglerFilesApi();                

                var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                {
                    From = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = SourceFilesystem
                    },
                    To = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = DestinationFilesystem
                    }
                };
                
                var exception = await AssertAsync.Throws<SmugglerException>(() => smugglerApi.Between(options));
                Assert.True(exception.Message.StartsWith("Smuggler does not support file system creation (file system '" + SourceFilesystem + "' on server"));

                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);

                exception = await AssertAsync.Throws<SmugglerException>(() => smugglerApi.Between(options));
                Assert.True(exception.Message.StartsWith("Smuggler does not support file system creation (file system '" + DestinationFilesystem + "' on server"));
            }
        }


        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_BehaviorWhenServerIsDown()
        {
            string dataDirectory = null;

            try
            {
                using (var store = NewStore())
                {
                    var server = GetServer();

                    var smugglerApi = new SmugglerFilesApi();

                    var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                    {
                        From = new FilesConnectionStringOptions
                        {
                            Url = "http://localhost:8078/",
                            DefaultFileSystem = SourceFilesystem
                        },
                        To = new FilesConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultFileSystem = DestinationFilesystem
                        }
                    };

                    await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);
                    await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(DestinationFilesystem);

                    var e = await AssertAsync.Throws<SmugglerException>(() => smugglerApi.Between(options));
                    Assert.Contains("Smuggler encountered a connection problem:", e.Message);

                    options.From.Url = store.Url;
                    options.To.Url = "http://localhost:8078/";

                    e = await AssertAsync.Throws<SmugglerException>(() => smugglerApi.Between(options));
                    Assert.Contains("Smuggler encountered a connection problem:", e.Message);

                    server.Dispose();
                }
            }
            finally
            {
                if (!string.IsNullOrWhiteSpace(dataDirectory))
                    IOExtensions.DeleteDirectory(dataDirectory);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_CanDumpEmptyFileSystem()
        {
            string dataDirectory = null;

            try
            {
                using (var store = NewStore())
                {
                    var server = GetServer();
                    dataDirectory = server.Configuration.DataDirectory;

                    var smugglerApi = new SmugglerFilesApi();

                    var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                    {
                        From = new FilesConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultFileSystem = SourceFilesystem
                        },
                        To = new FilesConnectionStringOptions
                        {
                            Url = store.Url,
                            DefaultFileSystem = DestinationFilesystem
                        }
                    };

                    await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);
                    await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(DestinationFilesystem);

                    await smugglerApi.Between(options);

                    using (var session = store.OpenAsyncSession(DestinationFilesystem))
                    {
                        var files = await session.Commands.BrowseAsync();
                        Assert.Equal(0, files.Count());
                    }

                    server.Dispose();
                }
            }
            finally
            {
                if (!string.IsNullOrWhiteSpace(dataDirectory))
                    IOExtensions.DeleteDirectory(dataDirectory);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_CanHandleFilesExceptionsGracefully()
        {
            using (var store = NewStore())
            {
                store.DefaultFileSystem = SourceFilesystem;

                var server = GetServer();

                var alreadyReset = false;
	            var port = 8070;
	            var forwarder = new ProxyServer(ref port, server.Configuration.Port)
                {
                    VetoTransfer = (totalRead, buffer) =>
                    {
	                    var s = Encoding.UTF8.GetString(buffer.Array, buffer.Offset, buffer.Count);
                        if (alreadyReset == false && totalRead > 28000 && !s.Contains("200 OK"))
                        {
                            alreadyReset = true;
                            return true;
                        }
                        return false;
                    }
                };

	            try
	            {
					var smugglerApi = new SmugglerFilesApi();

					var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
					{
						From = new FilesConnectionStringOptions
						{
							Url = "http://localhost:" + port,
							DefaultFileSystem = SourceFilesystem
						},
						To = new FilesConnectionStringOptions
						{
							Url = store.Url,
							DefaultFileSystem = DestinationFilesystem
						}
					};

					await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);
					await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(DestinationFilesystem);

					ReseedRandom(100); // Force a random distribution.

					await InitializeWithRandomFiles(store, 20, 30);


					Etag lastEtag = Etag.InvalidEtag;
					try
					{
						await smugglerApi.Between(options);
						Assert.False(true, "Expected error to happen during this Between operation, but it didn't happen :-(");
					}
					catch (SmugglerExportException inner)
					{
						lastEtag = inner.LastEtag;
					}

					Assert.NotEqual(Etag.InvalidEtag, lastEtag);

					await smugglerApi.Between(options);

					using (var session = store.OpenAsyncSession(DestinationFilesystem))
					{
						var files = await session.Commands.BrowseAsync();
						Assert.Equal(20, files.Count());
					}
	            }
	            finally
	            {
		            forwarder.Dispose();
					server.Dispose();
	            }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_ContentIsPreserved_SingleFile()
        {
            using (var store = NewStore())
            {
                var server = GetServer();

                ReseedRandom(100); // Force a random distribution.

                int fileSize = 10000;

                var smugglerApi = new SmugglerFilesApi();

                var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                {
                    From = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = SourceFilesystem
                    },
                    To = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = DestinationFilesystem
                    }
                };

                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(DestinationFilesystem);

                var fileContent = CreateRandomFileStream(fileSize);
                using (var session = store.OpenAsyncSession(SourceFilesystem))
                {
                    session.RegisterUpload("test1.file", fileContent);
                    await session.SaveChangesAsync();
                }

                await smugglerApi.Between(options);

                fileContent.Position = 0;
                using (var session = store.OpenAsyncSession(DestinationFilesystem))
                {
                    var file = session.LoadFileAsync("test1.file").Result;

                    Assert.Equal(fileSize, file.TotalSize);

                    var stream = session.DownloadAsync(file).Result;

                    Assert.Equal(fileContent.GetMD5Hash(), stream.GetMD5Hash());
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_ContentIsPreserved_MultipleFiles()
        {
            using (var store = NewStore())
            {
                var server = GetServer();

                int fileSize = 10000;

                var smugglerApi = new SmugglerFilesApi();

                var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                {
                    From = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = SourceFilesystem
                    },
                    To = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = DestinationFilesystem
                    }
                };

                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(DestinationFilesystem);

                var files = new Stream[10];
                using (var session = store.OpenAsyncSession(SourceFilesystem))
                {
                    for (int i = 0; i < files.Length; i++)
                    {
                        files[i] = CreateRandomFileStream(100 * i + 12);
                        session.RegisterUpload("test" + i + ".file", files[i]);
                    }

                    await session.SaveChangesAsync();
                }

                await smugglerApi.Between(options);

                for (int i = 0; i < files.Length; i++)
                {
                    using (var session = store.OpenAsyncSession(DestinationFilesystem))
                    {
                        var file = await session.LoadFileAsync("test" + i + ".file");
                        var stream = await session.DownloadAsync(file);

                        files[i].Position = 0;
                        Assert.Equal(files[i].GetMD5Hash(), stream.GetMD5Hash());
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_ContentIsPreserved_MultipleDirectories()
        {
            using (var store = NewStore())
            {
                var server = GetServer();

                int fileSize = 10000;

                var smugglerApi = new SmugglerFilesApi();

                var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                {
                    From = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = SourceFilesystem
                    },
                    To = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = DestinationFilesystem
                    }
                };

                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(DestinationFilesystem);

                var files = new Stream[10];
                using (var session = store.OpenAsyncSession(SourceFilesystem))
                {
                    for (int i = 0; i < files.Length; i++)
                    {
                        files[i] = CreateRandomFileStream(100 * i + 12);
                        session.RegisterUpload(i + "/test.file", files[i]);
                    }

                    await session.SaveChangesAsync();
                }

                await smugglerApi.Between(options);

                for (int i = 0; i < files.Length; i++)
                {
                    using (var session = store.OpenAsyncSession(DestinationFilesystem))
                    {
                        var file = await session.LoadFileAsync(i + "/test.file");
                        var stream = await session.DownloadAsync(file);

                        files[i].Position = 0;
                        Assert.Equal(files[i].GetMD5Hash(), stream.GetMD5Hash());
                    }
                }
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_OnlyActiveContentIsPreserved_MultipleDirectories()
        {
            using (var store = NewStore())
            {
                var server = GetServer();

                var smugglerApi = new SmugglerFilesApi();

                var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                {
                    From = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = SourceFilesystem
                    },
                    To = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = DestinationFilesystem
                    }
                };

                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(DestinationFilesystem);

                int total = 10;
                var files = new string[total];
                using (var session = store.OpenAsyncSession(SourceFilesystem))
                {
                    for (int i = 0; i < total; i++)
                    {
                        files[i] = i + "/test.file";
                        session.RegisterUpload(files[i], CreateRandomFileStream(100 * i + 12));
                    }

                    await session.SaveChangesAsync();
                }

                int deletedFiles = 0;
                var rnd = new Random();
                using (var session = store.OpenAsyncSession(SourceFilesystem))
                {
                    for (int i = 0; i < total; i++)
                    {
                        if (rnd.Next(2) == 0)
                        {
                            session.RegisterFileDeletion(files[i]);
                            deletedFiles++;
                        }

                    }

                    await session.SaveChangesAsync();
                }

                await smugglerApi.Between(options);

                using (var session = store.OpenAsyncSession(DestinationFilesystem))
                {
                    int activeFiles = 0;
                    for (int i = 0; i < total; i++)
                    {
                        var file = await session.LoadFileAsync(files[i]);
                        if (file != null)
                            activeFiles++;
                    }

                    Assert.Equal(total - deletedFiles, activeFiles);
                }
            }
        }


        [Fact, Trait("Category", "Smuggler")]
        public async Task BetweenOperation_MetadataIsPreserved()
        {
            using (var store = NewStore())
            {
                var server = GetServer();

                int fileSize = 10000;

                var smugglerApi = new SmugglerFilesApi();

                var options = new SmugglerBetweenOptions<FilesConnectionStringOptions>
                {
                    From = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = SourceFilesystem
                    },
                    To = new FilesConnectionStringOptions
                    {
                        Url = store.Url,
                        DefaultFileSystem = DestinationFilesystem
                    }
                };

                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(SourceFilesystem);
                await store.AsyncFilesCommands.Admin.EnsureFileSystemExistsAsync(DestinationFilesystem);

                FileHeader originalFile;
                using (var session = store.OpenAsyncSession(SourceFilesystem))
                {
                    session.RegisterUpload("test1.file", CreateRandomFileStream(12800));
                    await session.SaveChangesAsync();

                    // content update after a metadata change
                    originalFile = await session.LoadFileAsync("test1.file");
                    originalFile.Metadata["Test"] = new RavenJValue("Value");
                    await session.SaveChangesAsync();
                }

                await smugglerApi.Between(options);

                using (var session = store.OpenAsyncSession(DestinationFilesystem))
                {
                    var file = await session.LoadFileAsync("test1.file");

                    Assert.Equal(originalFile.CreationDate, file.CreationDate);
                    Assert.Equal(originalFile.Directory, file.Directory);
                    Assert.Equal(originalFile.Extension, file.Extension);
                    Assert.Equal(originalFile.FullPath, file.FullPath);
                    Assert.Equal(originalFile.Name, file.Name);
                    Assert.Equal(originalFile.TotalSize, file.TotalSize);
                    Assert.Equal(originalFile.UploadedSize, file.UploadedSize);
                    Assert.Equal(originalFile.LastModified, file.LastModified);

                    Assert.True(file.Metadata.ContainsKey("Test"));
                }              
            }
        }
    }
}
