// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4208.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4208 : RavenFilesTestBase
    {
        [Fact]
        public async Task streaming_of_file_headers_respects_pageSize_parameter()
        {
            using (var store = NewStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 20; i++)
                        session.RegisterUpload(i + ".file", CreateRandomFileStream(5));

                    await session.SaveChangesAsync();
                }
                
                using (var session = store.OpenAsyncSession())
                {
                    int count = 0;

                    using (var reader = await session.Commands.StreamFileHeadersAsync(Etag.Empty, pageSize: 10))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            Assert.IsType<FileHeader>(reader.Current);
                        }
                    }

                    Assert.Equal(10, count);
                }
            } 
        }

        [Fact]
        public async Task streaming_of_file_headers_can_return_fewer_results_than_specified_pageSize_parameter_if_there_is_no_more_files()
        {
            using (var store = NewStore())
            {
                Etag etagOfFifteenthFile;
                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 0; i < 20; i++)
                        session.RegisterUpload(i + ".file", CreateRandomFileStream(5));

                    await session.SaveChangesAsync();

                    var fifeteenth = await session.LoadFileAsync("14.file");
                    etagOfFifteenthFile = fifeteenth.Etag;
                }

                using (var session = store.OpenAsyncSession())
                {
                    int count = 0;

                    using (var reader = await session.Commands.StreamFileHeadersAsync(etagOfFifteenthFile, pageSize: 10))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            Assert.IsType<FileHeader>(reader.Current);
                        }
                    }

                    Assert.Equal(5, count);
                }
            }
        }

        [Fact]
        public async Task streaming_of_file_headers_does_not_return_deleting_files_RavenDB_3047_and_respects_pageSize_parameter()
        {
            using (var store = NewStore())
            {
                for (int i = 0; i < 20; i++)
                {
                    await store.AsyncFilesCommands.UploadAsync(i + ".file", CreateRandomFileStream(5));
                    await store.AsyncFilesCommands.UploadAsync(i + ".file", CreateRandomFileStream(5)); // override is going to create .deleting file
                }

                for (int i = 20; i < 33; i++)
                {
                    await store.AsyncFilesCommands.UploadAsync(i + ".file", CreateRandomFileStream(5));
                }

                using (var session = store.OpenAsyncSession())
                {
                    int count = 0;

                    using (var reader = await session.Commands.StreamFileHeadersAsync(Etag.Empty, pageSize: 10))
                    {
                        while (await reader.MoveNextAsync())
                        {
                            count++;
                            Assert.IsType<FileHeader>(reader.Current);
                        }
                    }

                    Assert.Equal(10, count);
                }
            }
        }
    }
}