using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Replication;
using Raven.Client.FileSystem;
using Raven.Client.FileSystem.Connection;
using Raven.Client.FileSystem.Extensions;
using Raven.Json.Linq;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;
using Raven.Abstractions.Util;
using Raven.Server;

namespace Raven.Tests.FileSystem
{
    public class CommandsUsage : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task Can_update_just_metadata()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;
            var client = NewAsyncClient();
            await client.UploadAsync("abc.txt", ms, new RavenJObject
                                                {
                                                    {"test", "1"}
                                                });

            await client.UpdateMetadataAsync("abc.txt", new RavenJObject
                                                        {
                                                            {"test", "2"}
                                                        });


            var metadata = await client.GetMetadataForAsync("abc.txt");
            Assert.Equal("2", metadata["test"]);

            var readStream = await client.DownloadAsync("abc.txt");
            Assert.Equal(expected, StreamToString(readStream));
        }

        [Fact]
        public async Task Can_get_partial_results()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            for (var i = 0; i < 1024*8; i++)
            {
                streamWriter.Write(i);
                streamWriter.Write(",");
            }
            streamWriter.Flush();
            ms.Position = 0;
            var client = NewAsyncClient();
            await client.UploadAsync("numbers.txt", ms);

            var actual = await client.DownloadAsync("numbers.txt", null, 1024*4 + 1);
            ms.Position = 1024*4 + 1;
            var expectedString = new StreamReader(ms).ReadToEnd();
            var actualString = new StreamReader(actual).ReadToEnd();

            Assert.Equal(expectedString, actualString);
        }


        [Theory]
        [InlineData(1024*1024)] // 1 mb
        [InlineData(1024*1024*8)] // 8 mb
        public async Task Can_upload(int size)
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', size);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;

            var client = NewAsyncClient();
            await client.UploadAsync("abc.txt", ms);

            var stream = await client.DownloadAsync("abc.txt");
            Assert.Equal(expected, StreamToString(stream));
        }

        [Fact]
        public async Task Can_upload_metadata_and_head_metadata()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;
            var client = NewAsyncClient();
            await client.UploadAsync("abc.txt", ms, new RavenJObject
                                                {
                                                    {"test", "value"},
                                                    {"hello", "there"}
                                                });


            var collection = await client.GetMetadataForAsync("abc.txt");

            Assert.Equal("value", collection["test"]);
            Assert.Equal("there", collection["hello"]);
        }


        [Fact]
        public async Task Can_query_metadata()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;
            var client = NewAsyncClient();
            await client.UploadAsync("abc.txt", ms, new RavenJObject
                                                {
                                                    {"Test", "value"},
                                                });


            var collection = await client.SearchAsync("Test:value");

            Assert.Equal(1, collection.Files.Count);
            Assert.Equal("abc.txt", collection.Files[0].Name);
            Assert.Equal("value", collection.Files[0].Metadata["Test"]);
        }


        [Fact]
        public async Task Can_download()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;
            var client = NewAsyncClient();
            await client.UploadAsync("abc.txt", ms);

            var ms2 = await client.DownloadAsync("abc.txt");
            var actual = new StreamReader(ms2).ReadToEnd();

            Assert.Equal(expected, actual);
        }

        [Fact]
        public async Task Can_check_rdc_stats()
        {
            var client = NewAsyncClient();
            var result = await client.Synchronization.GetRdcStatsAsync();
            Assert.NotNull(result);
            Assert.True(result.CurrentVersion > 0);
            Assert.True(result.MinimumCompatibleAppVersion > 0);
            Assert.True(result.CurrentVersion >= result.MinimumCompatibleAppVersion);
        }

        [Fact]
        public async Task Can_get_rdc_manifest()
        {
            var client = NewAsyncClient();

            var buffer = new byte[1024*1024];
            new Random().NextBytes(buffer);

            await client.UploadAsync("mb.bin", new MemoryStream(buffer));


            var result = await client.Synchronization.GetRdcManifestAsync("mb.bin");
            Assert.NotNull(result);
        }

        [Fact]
        public async Task Can_get_rdc_signatures()
        {
            var client = NewAsyncClient();

            var buffer = new byte[1024*1024*2];
            new Random().NextBytes(buffer);

            await client.UploadAsync("mb.bin", new MemoryStream(buffer));

            var result = await client.Synchronization.GetRdcManifestAsync("mb.bin");

            Assert.True(result.Signatures.Count > 0);

            foreach (var item in result.Signatures)
            {
                var ms = new MemoryStream();
                await client.Synchronization.DownloadSignatureAsync(item.Name, ms);
                Assert.True(ms.Length == item.Length);
            }
        }

        [Fact]
        public async Task Can_get_rdc_signature_partialy()
        {
            var client = NewAsyncClient();
            var buffer = new byte[1024*1024*4];
            new Random().NextBytes(buffer);

            await client.UploadAsync("mb.bin", new MemoryStream(buffer));
            var signatureManifest = await client.Synchronization.GetRdcManifestAsync("mb.bin");

            var ms = new MemoryStream();
            await client.Synchronization.DownloadSignatureAsync(signatureManifest.Signatures[0].Name, ms, 5, 10);
            Assert.Equal(5, ms.Length);
        }

        [Fact]
        public async Task Can_get_partial_content_from_the_begin()
        {
            var ms = PrepareTextSourceStream();
            var client = NewAsyncClient();
            await client.UploadAsync("abc.txt", ms, new RavenJObject
                                   {
                                       {"test", "1"}
                                   });

            var nameValues = new Reference<RavenJObject>();
            var downloadedStream = await client.DownloadAsync("abc.txt", nameValues, 0, 6);

            var result = new StreamReader(downloadedStream).ReadToEnd();

            Assert.Equal("000001", result);
            Assert.Equal("bytes 0-5/3000000", nameValues.Value["Content-Range"]);			
        }

        [Fact]
        public async Task Can_get_partial_content_from_the_middle()
        {
            var ms = PrepareTextSourceStream();
            var client = NewAsyncClient();
            await client.UploadAsync("abc.txt", ms, new RavenJObject
                                       {
                                           {"test", "1"}
                                       });

            var nameValues = new Reference<RavenJObject>();
            var downloadedStream = await client.DownloadAsync("abc.txt", nameValues, 3006, 3017);
            
            var result = new StreamReader(downloadedStream).ReadToEnd();
            Assert.Equal("00050200050", result);
            Assert.Equal("bytes 3006-3016/3000000", nameValues.Value["Content-Range"]);
        }

        [Fact]
        public async Task Can_get_partial_content_from_the_end_explicitely()
        {
            var ms = PrepareTextSourceStream();
            var client = NewAsyncClient();
            await client.UploadAsync("abc.txt", ms, new RavenJObject
                                   {
                                       {"test", "1"}
                                   });


            var nameValues = new Reference<RavenJObject>();
            var downloadedStream = await client.DownloadAsync("abc.txt", nameValues, ms.Length - 6, ms.Length - 1);
           
            var result = new StreamReader(downloadedStream).ReadToEnd();

            Assert.Equal("50000", result);
            Assert.Equal("bytes 2999994-2999998/3000000", nameValues.Value.Value<string>("Content-Range"));			
        }

        [Fact]
        public async Task Can_get_partial_content_from_the_end()
        {
            var ms = PrepareTextSourceStream();
            var client = NewAsyncClient();
            await client.UploadAsync("abc.bin", ms, new RavenJObject
                                   {
                                       {"test", "1"}
                                   });

            var nameValues = new Reference<RavenJObject>();
            var downloadedStream = await client.DownloadAsync("abc.bin", nameValues, ms.Length - 7);
            var result = new StreamReader(downloadedStream).ReadToEnd();

            Assert.Equal("9500000", result);
            Assert.Equal("bytes 2999993-2999999/3000000", nameValues.Value.Value<string>("Content-Range"));
        }

        [Fact]
        public async Task Should_modify_etag_after_upload()
        {
            var client = NewAsyncClient();

            // note that file upload modifies ETag twice and indicates file to delete what creates another etag for tombstone
            await client.UploadAsync("test.bin", new RandomStream(10), new RavenJObject());
            var resultFileMetadata = await client.GetMetadataForAsync("test.bin");
            var etag0 = Etag.Parse(resultFileMetadata.Value<string>(Constants.MetadataEtagField));
            await client.UploadAsync("test.bin", new RandomStream(10), new RavenJObject());
            resultFileMetadata = await client.GetMetadataForAsync("test.bin");
            var etag1 = Etag.Parse(resultFileMetadata.Value<string>(Constants.MetadataEtagField));
            
            Assert.Equal(Etag.Parse("00000000-0000-0001-0000-000000000002"), etag0);
            Assert.Equal(Etag.Parse("00000000-0000-0001-0000-000000000005"), etag1);
            Assert.True(etag1.CompareTo(etag0) > 0, "ETag after second update should be greater");
        }

        [Fact]
        public async Task Should_not_see_already_deleted_files()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("visible.bin", new RandomStream(1));
            await client.UploadAsync("toDelete.bin", new RandomStream(1));

            await client.DeleteAsync("toDelete.bin");

            var fileInfos = await client.BrowseAsync();
            Assert.Equal(1, fileInfos.Length);
            Assert.Equal("visible.bin", fileInfos[0].Name);
        }

        [Fact]
        public async Task Should_not_return_metadata_of_deleted_file()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("toDelete.bin", new RandomStream(1));

            await client.DeleteAsync("toDelete.bin");

            var metadata = await client.GetMetadataForAsync("toDelete.bin");
            Assert.Null(metadata);
        }

        [Fact]
        public void File_system_stats_after_file_delete()
        {
            var client = NewAsyncClient();
            client.UploadAsync("toDelete.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

            client.DeleteAsync("toDelete.bin").Wait();

            Assert.Equal(0, client.GetStatisticsAsync().Result.FileCount);
        }

        [Fact]
        public void File_system_stats_after_rename()
        {
            var client = NewAsyncClient();
            client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5})).Wait();

            client.RenameAsync("file.bin", "renamed.bin").Wait();

            Assert.Equal(1, client.GetStatisticsAsync().Result.FileCount);
        }

        [Fact]
        public void File_system_stats_after_copy()
        {
            var client = NewAsyncClient();
            client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 })).Wait();

            client.CopyAsync("file.bin", "newName.bin").Wait();

            Assert.Equal(2, client.GetStatisticsAsync().Result.FileCount);
        }

        [Fact]
        public async Task Can_back_to_previous_name()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            await client.RenameAsync("file.bin", "renamed.bin");
            await client.RenameAsync("renamed.bin", "file.bin");

            var files = await client.BrowseAsync();
            Assert.Equal("file.bin", files[0].Name);
        }

        [Fact]
        public async Task Can_upload_file_with_the_same_name_as_previously_deleted()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            await client.DeleteAsync("file.bin");
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            var files = await client.BrowseAsync();
            Assert.Equal("file.bin", files[0].Name);
        }

        [Fact]
        public async Task Can_upload_file_with_the_same_name_as_previously_renamed()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            await client.RenameAsync("file.bin", "renamed.bin");
            await client.UploadAsync("file.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }));

            var files = await client.BrowseAsync();
            Assert.Equal(2, files.Length);
            Assert.True("file.bin" == files[0].Name || "renamed.bin" == files[0].Name);
            Assert.True("file.bin" == files[1].Name || "renamed.bin" == files[1].Name);
        }

        [Fact]
        public async Task Should_refuse_to_rename_if_file_with_the_same_name_already_exists()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("file1.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5}));
            await client.UploadAsync("file2.bin", new MemoryStream(new byte[] {1, 2, 3, 4, 5}));

            Exception ex = null;
            try
            {
                await client.RenameAsync("file1.bin", "file2.bin");
            }
            catch (ErrorResponseException e)
            {
                ex = e.GetBaseException();
            }
            Assert.Contains(string.Format("Cannot rename because file {0} already exists", FileHeader.Canonize("file2.bin")), ex.Message);
        }

        [Fact]
        public async Task Can_copy_file()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("file1.bin", new MemoryStream(new byte[] { 1, 2, 3, 4, 5 }), new RavenJObject
            {
                {"first", "aa"},
                {"second", "bb"}
            });
            await client.CopyAsync("file1.bin", "file2.bin");
            var files = await client.BrowseAsync();
            Assert.Equal(2, files.Length);
            Assert.True("file1.bin" == files[0].Name || "file2.bin" == files[0].Name);
            Assert.True("file2.bin" == files[1].Name || "file1.bin" == files[1].Name);

            var metadata = await client.GetMetadataForAsync("file2.bin");
            Assert.Equal("aa", metadata.Value<string>("first"));
            Assert.Equal("bb", metadata.Value<string>("second"));
        }

        [Fact]
        public void Can_upload_file_with_hash_in_name()
        {
            var client = NewAsyncClient();

            client.UploadAsync("name#.bin", new MemoryStream(new byte[] {1, 2, 3})).Wait();

            Assert.NotNull(client.GetMetadataForAsync("name#.bin").Result);
        }

        [Fact]
        public void Can_query_file_with_hash_in_name()
        {
            var client = NewAsyncClient();

            client.UploadAsync("name#.bin", new MemoryStream(new byte[] { 1, 2, 3 })).Wait();

            var results = client.SearchAsync("__rfileName:nib.#eman*").Result;

            Assert.Equal(1, results.FileCount);
            Assert.Equal("name#.bin", results.Files[0].Name);
        }

        private void ExecuteWithSimplifiedException ( Action action )
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                throw e.SimplifyException();
            }
        }

        [Fact]
        public void Should_throw_file_not_found_exception()
        {
            var client = NewAsyncClient();

            Assert.Throws<FileNotFoundException>(() => ExecuteWithSimplifiedException(() => client.DownloadAsync("not_existing_file").Wait()));
            Assert.Throws<FileNotFoundException>(() => ExecuteWithSimplifiedException(() => client.RenameAsync("not_existing_file", "abc").Wait()));
            Assert.Throws<FileNotFoundException>(() => ExecuteWithSimplifiedException(() => client.DeleteAsync("not_existing_file").Wait()));
            Assert.Throws<FileNotFoundException>(() => ExecuteWithSimplifiedException(() => client.UpdateMetadataAsync("not_existing_file", new RavenJObject()).Wait()));
        }

        [Fact]
        public async Task Must_not_rename_tombstone()
        {
            var client = NewAsyncClient();

            await client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3}));
            await client.RenameAsync("file.bin", "newname.bin");

            try
            {
                await client.RenameAsync("file.bin", "file2.bin");
                Assert.Equal(true, false); // Should not get here
            }
            catch (Exception ex)
            {
                Assert.IsType<FileNotFoundException>(ex.GetBaseException());
            }
        }

        [Fact]
        public async Task Next_file_delete_should_throw_file_not_found_exception()
        {
            var client = NewAsyncClient();

            await client.UploadAsync("file.bin", new MemoryStream(new byte[] {1, 2, 3}));
            await client.DeleteAsync("file.bin");

            try
            {
                await client.DeleteAsync("file.bin");
                Assert.Equal(true, false); // Should not get here
            }
            catch (Exception ex)
            {
                Assert.IsType<FileNotFoundException>(ex.GetBaseException());
            }
        }

        [Fact]
        public async Task Can_get_stats_for_all_active_file_systems()
        {
            var store = NewStore();
            var failoverConvention = store.Conventions.FailoverBehavior;
            store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
            
            try
            {
                using (var client = store.AsyncFilesCommands)
                using (var anotherClient = client.ForFileSystem("test"))
                {
                    await anotherClient.EnsureFileSystemExistsAsync();

                    await client.UploadAsync("test1", new RandomStream(10)); // will make it active
                    await anotherClient.UploadAsync("test1", new RandomStream(10)); // will make it active

                    await client.UploadAsync("test2", new RandomStream(10));

                    var stats = await anotherClient.Admin.GetStatisticsAsync();

                    var stats1 = stats.FirstOrDefault(x => x.Name == client.FileSystemName);
                    Assert.NotNull(stats1);
                    var stats2 = stats.FirstOrDefault(x => x.Name == anotherClient.FileSystemName);
                    Assert.NotNull(stats2);

                    Assert.Equal(2, stats1.Metrics.Requests.Count);
                    Assert.Equal(1, stats2.Metrics.Requests.Count);

                    Assert.Equal(0, stats1.ActiveSyncs.Count);
                    Assert.Equal(0, stats1.PendingSyncs.Count);

                    Assert.Equal(0, stats2.ActiveSyncs.Count);
                    Assert.Equal(0, stats2.PendingSyncs.Count);
                }
            }
            finally
            {
                store.Conventions.FailoverBehavior= failoverConvention;
            }

            
        }
        protected override void ModifyStore(FilesStore store)
        {
            store.Conventions.FailoverBehavior = FailoverBehavior.FailImmediately;
        }

        [Fact]
        public async Task Will_not_return_stats_of_inactive_file_systems()
        {
            var client = NewAsyncClient(); // will create a file system but it remain inactive until any request will go there

            var stats = (await client.Admin.GetStatisticsAsync()).FirstOrDefault();

            Assert.Null(stats);
        }

        [Fact]
        public async Task CanCreateAndDeleteFileSystem()
        {
            var client = (IAsyncFilesCommandsImpl)NewAsyncClient();
            var adminClient = client.Admin;

            const string newFileSystemName = "testName_CanDeleteFileSystem";

            await adminClient.CreateOrUpdateFileSystemAsync(new FileSystemDocument
            {
                Id = "Raven/FileSystem/" + newFileSystemName,
                Settings =
                 {
                     {Constants.FileSystem.DataDirectory, Path.Combine("~", Path.Combine("FileSystems", newFileSystemName))}
                 }
            }, newFileSystemName);

            using (var createdFsClient = new AsyncFilesServerClient(client.ServerUrl, newFileSystemName))
            {
                await createdFsClient.UploadAsync("foo", new MemoryStream(new byte[] { 1 }));
            }

            var names = await adminClient.GetNamesAsync();

            Assert.Contains(newFileSystemName, names);

            var stats = await adminClient.GetStatisticsAsync();

            Assert.NotNull(stats.FirstOrDefault(x => x.Name == newFileSystemName));

            await adminClient.DeleteFileSystemAsync(newFileSystemName);

            names = await adminClient.GetNamesAsync();

            Assert.DoesNotContain(newFileSystemName, names);
        }

        [Fact]
        public async Task CanCreateFileSystemWithDefaultValues()
        {
            var client = (IAsyncFilesCommandsImpl)NewAsyncClient();
            var adminClient = client.Admin;

            const string newFileSystemName = "testName_CanCreateFileSystemWithDefaultValues";

            await adminClient.CreateOrUpdateFileSystemAsync(new FileSystemDocument(), newFileSystemName);

            using (var createdFsClient = new AsyncFilesServerClient(client.ServerUrl, newFileSystemName))
            {
                await createdFsClient.UploadAsync("foo", new MemoryStream(new byte[] { 1 }));
            }

            var names = await adminClient.GetNamesAsync();

            Assert.Contains(newFileSystemName, names);

            var stats = await adminClient.GetStatisticsAsync();

            Assert.NotNull(stats.FirstOrDefault(x => x.Name == newFileSystemName));
        }

        [Fact]
        public async Task CreateFileSystemWhenExistingWillFail()
        {
            var client = NewAsyncClient();
            var adminClient = client.Admin;

            const string newFileSystemName = "testName_CreateFileSystemWhenExistingWillFail";

            var fileSystemSpec = new FileSystemDocument
            {
                Id = Constants.FileSystem.Prefix + newFileSystemName,
                Settings =
                 {
                     {Constants.FileSystem.DataDirectory, Path.Combine("~", Path.Combine("FileSystems", newFileSystemName))}
                 }
            };

            await adminClient.CreateFileSystemAsync(fileSystemSpec);

            var names = await adminClient.GetNamesAsync();
            Assert.Contains(newFileSystemName, names);
            Assert.Throws<InvalidOperationException>(()=>AsyncHelpers.RunSync(() => adminClient.CreateFileSystemAsync(fileSystemSpec)));

        }

        [Fact]
        public async Task Can_get_files_metadata()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("1.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            await client.UploadAsync("a/b/2.txt", new RandomStream(128));

            var fileMetadata = await client.GetAsync(new string[] { "1.txt", "a/b/2.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(2, fileMetadata.Length);
            Assert.Equal("1.txt", fileMetadata[0].Name);
            Assert.Equal("/a/b/2.txt", fileMetadata[1].FullPath);
            Assert.Equal("2.txt", fileMetadata[1].Name);
            Assert.Equal(128, fileMetadata[0].TotalSize);
            Assert.Equal(128, fileMetadata[1].TotalSize);
            Assert.NotNull(fileMetadata[0].Etag);
            Assert.NotNull(fileMetadata[1].Etag);

            Assert.NotNull(fileMetadata[0].LastModified);
            Assert.Equal(fileMetadata[0].Metadata[Constants.LastModified].Value<DateTimeOffset>(), fileMetadata[0].LastModified);
            Assert.NotNull(fileMetadata[0].CreationDate);
            Assert.Equal(fileMetadata[1].Metadata[Constants.CreationDate].Value<DateTimeOffset>(), fileMetadata[1].CreationDate);
            Assert.Equal(fileMetadata[1].Metadata[Constants.RavenCreationDate].Value<DateTimeOffset>(), fileMetadata[1].CreationDate);
            Assert.Equal(".txt", fileMetadata[0].Extension);
            Assert.Equal(".txt", fileMetadata[1].Extension);
            Assert.Equal("/", fileMetadata[0].Directory);
            Assert.Equal("/a/b", fileMetadata[1].Directory);
        }

        private static MemoryStream PrepareTextSourceStream()
        {
            var ms = new MemoryStream();
            var writer = new StreamWriter(ms);
            for (var i = 1; i <= 500000; i++)
            {
                writer.Write(i.ToString("D6"));
            }
            writer.Flush();
            ms.Position = 0;
            return ms;
        }

        [Fact]
        public async Task Can_Handle_Upload_With_Etag()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("1.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            var fileMetadata = await client.GetAsync(new [] {"1.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(1, fileMetadata.Length);

            var etag = fileMetadata[0].Etag;

            await client.UploadAsync("1.txt", new RandomStream(256),
                                        new RavenJObject
                                        {
                                            {"test", "2"}
                                        }, etag: etag);

            Assert.Throws<ConcurrencyException>(() => AsyncHelpers.RunSync(() => client.UploadAsync("1.txt", new RandomStream(256), etag: etag.IncrementBy(10))));
        }

        [Fact]
        public async Task CanDelete()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("1.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            await client.UploadAsync("2.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            var fileMetadata = await client.GetAsync(new[] { "1.txt", "2.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(2, fileMetadata.Length);

            var etag1 = fileMetadata[0].Etag;
            await client.DeleteAsync("1.txt", etag1);

            var deletedFileMetadata = await client.GetMetadataForAsync("1.txt");

            Assert.Null(deletedFileMetadata);

            fileMetadata = await client.GetAsync(new[] { "1.txt", "2.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(2, fileMetadata.Length);
            Assert.Null(fileMetadata[0]);
        }

        [Fact]
        public async Task Can_Handle_Delete_With_Etag()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("1.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            await client.UploadAsync("2.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            var fileMetadata = await client.GetAsync(new[] { "1.txt", "2.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(2, fileMetadata.Length);

            var etag1 = fileMetadata[0].Etag;
            await client.DeleteAsync("1.txt", etag1);

            var ex = Assert.Throws<ConcurrencyException>(() => AsyncHelpers.RunSync(() => client.DeleteAsync("2.txt", etag1 /* we are using wrong etag here */)));

            Assert.Equal("Operation attempted on file '/2.txt' using a non current etag", ex.Message);

            Assert.NotNull(ex.ExpectedETag);
            Assert.NotNull(ex.ActualETag);

            fileMetadata = await client.GetAsync(new[] { "1.txt", "2.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(2, fileMetadata.Length);
            Assert.Null(fileMetadata[0]);
            Assert.NotNull(fileMetadata[1]);
        }

        [Fact]
        public async Task Can_Handle_Rename_With_Etag()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("1.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            await client.UploadAsync("2.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            var fileMetadata = await client.GetAsync(new[] { "1.txt", "2.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(2, fileMetadata.Length);

            var etag1 = fileMetadata[0].Etag;
            await client.RenameAsync("1.txt", "1.new.txt", etag1);

            Assert.Throws<ConcurrencyException>(() => AsyncHelpers.RunSync(() => client.RenameAsync("2.txt", "2.new.txt", etag1 /* we are using wrong etag here */)));

            fileMetadata = await client.GetAsync(new[] { "1.txt", "1.new.txt", "2.txt", "2.new.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(4, fileMetadata.Length);
            Assert.Null(fileMetadata[0]);
            Assert.NotNull(fileMetadata[1]);
            Assert.NotNull(fileMetadata[2]);
            Assert.Null(fileMetadata[3]);
        }

        [Fact]
        public async Task Can_Handle_Metadata_Update_With_Etag()
        {
            var client = NewAsyncClient();
            await client.UploadAsync("1.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "1"}
                                        });

            await client.UploadAsync("2.txt", new RandomStream(128),
                                        new RavenJObject
                                        {
                                            {"test", "2"}
                                        });

            var fileMetadata = await client.GetAsync(new[] { "1.txt", "2.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(2, fileMetadata.Length);

            var etag1 = fileMetadata[0].Etag;
            await client.UpdateMetadataAsync("1.txt", new RavenJObject
            {
                {"test-new", "1"}
            }, etag1);

            Assert.Throws<ConcurrencyException>(() => AsyncHelpers.RunSync(() => client.UpdateMetadataAsync("2.txt", new RavenJObject
            {
                {"test-new2", "4"}
            }, etag1 /* we are using wrong etag here */)));

            fileMetadata = await client.GetAsync(new[] { "1.txt", "2.txt" });
            Assert.NotNull(fileMetadata);
            Assert.Equal(2, fileMetadata.Length);
            Assert.Null(fileMetadata[0].Metadata["test"]);
            Assert.NotNull(fileMetadata[0].Metadata["test-new"]);
            Assert.NotNull(fileMetadata[1].Metadata["test"]);
            Assert.Null(fileMetadata[1].Metadata["test-new2"]);
        }
    }
}
