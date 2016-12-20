// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3955.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.FileSystem;
using Raven.Database.Extensions;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_3955 : RavenFilesTestWithLogs
    {
        [Theory]
        [PropertyData("Storages")]
        public async Task uploading_file_multiple_times_must_not_throw_key_duplicate_exception_on_esent_and_concurrency_exception_on_voron(string storage)
        {
            var r = new Random(1);
            var bytes = new byte[1024];

            r.NextBytes(bytes);

            var ms = new MemoryStream(bytes);
            var expectedHash = ms.GetMD5Hash();

            var client = NewAsyncClient(requestedStorage: storage);
            for (int i = 0; i < 500; i++)
            {
                ms.Position = 0;
                await client.UploadAsync("abc.bin", ms);
            }
            
            var stream = await client.DownloadAsync("abc.bin");
            
            Assert.Equal(expectedHash, stream.GetMD5Hash());
        }
    }
}
