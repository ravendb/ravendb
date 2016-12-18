// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2784.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
    public class RavenDB_FEI_2 : RavenFilesTestWithLogs
    {
        private const int Size = 1024*1024; //1mb

        [Theory]
        [InlineData("voron")]
        [InlineData("esent")]
        public async Task Can_Download_File_Case_Sensitive(string storage)
        {
            var client = NewAsyncClient(requestedStorage: storage);
            await client.UploadAsync("AbC.txt", new RandomStream(Size));

            var downloadData = new MemoryStream();
            (await client.DownloadAsync("aBc.TxT")).CopyTo(downloadData);

            Assert.Equal(Size, downloadData.Length);
        }

        [Theory]
        [InlineData("voron")]
        [InlineData("esent")]
        public async Task Can_Rename_File_Case_Sensitive(string storage)
        {
            var client = NewAsyncClient(requestedStorage: storage);
            await client.UploadAsync("AbC.txt", new RandomStream(Size));

            await client.RenameAsync("abc.txt", "text.txt");

            var downloadData = new MemoryStream();
            (await client.DownloadAsync("text.txt")).CopyTo(downloadData);

            Assert.Equal(Size, downloadData.Length);
        }
    }
}
