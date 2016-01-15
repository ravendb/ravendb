// -----------------------------------------------------------------------
//  <copyright file="RavenDB_4206.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_4206 : RavenFilesTestWithLogs
    {
        [Fact]
        public async Task can_stream_more_than_1024_docs()
        {
            using (var store = NewStore())
            {
                var memoryStream = new MemoryStream();

                for (int i = 0; i < 1500; i++)
                {
                    memoryStream.Position = 0;

                    await store.AsyncFilesCommands.UploadAsync("file-" + i, memoryStream);
                }

                var reader = await store.AsyncFilesCommands.StreamFileHeadersAsync(Etag.Empty);

                int count = 0;

                while (await reader.MoveNextAsync())
                {
                    count++;
                }

                Assert.Equal(1500, count);
            }
        }
    }
}