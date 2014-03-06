// -----------------------------------------------------------------------
//  <copyright file="ClientWithAuth.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace RavenFS.Tests.Auth
{
    public class ClientWithAuth : RavenFsTestBase
    {
        [Fact]
        public async Task Can_download()
        {
            var ms = new MemoryStream();
            var streamWriter = new StreamWriter(ms);
            var expected = new string('a', 1024);
            streamWriter.Write(expected);
            streamWriter.Flush();
            ms.Position = 0;
            var client = NewClient();

            //var server = GetServer().SystemDatabase.Put()

            await client.UploadAsync("abc.txt", ms);

            var ms2 = new MemoryStream();
            await client.DownloadAsync("abc.txt", ms2);

            ms2.Position = 0;

            var actual = new StreamReader(ms2).ReadToEnd();

            Assert.Equal(expected, actual);
        }
    }
}