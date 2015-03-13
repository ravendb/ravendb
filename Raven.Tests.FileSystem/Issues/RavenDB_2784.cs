// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2784.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Tests.FileSystem.Synchronization.IO;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Issues
{
	public class RavenDB_2784 : RavenFilesTestWithLogs
	{
		[Theory]
		[InlineData("voron")]
		[InlineData("esent")]
		public async Task DownloadingWithZeroSize(string storage)
		{
			var client = NewAsyncClient(requestedStorage: storage);

			await client.UploadAsync("file", new RandomStream(512 * 1024, 1));
			await client.UploadAsync("file", new RandomStream(512 * 1024, 1));

			await client.Storage.CleanUpAsync();

			var fileHeader = await client.GetMetadataForAsync("file");

			using (var stream = await client.DownloadAsync("file"))
			{
				var downloadData = new MemoryStream();
				stream.CopyTo(downloadData);

				Assert.Equal(fileHeader.Value<long>(Constants.FileSystem.RavenFsSize), downloadData.Length);
			}
		}
	}
}