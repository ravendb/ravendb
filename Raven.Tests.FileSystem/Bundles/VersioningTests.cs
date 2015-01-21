// -----------------------------------------------------------------------
//  <copyright file="VersioningTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Threading.Tasks;

using Raven.Bundles.Versioning.Data;
using Raven.Database.FileSystem.Bundles.Versioning;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Bundles
{
	public class VersioningTests : RavenFilesTestWithLogs
	{
		[Theory]
		[PropertyData("Storages")]
		public async Task Simple(string requestedStorage)
		{
			const string FileName = "file1.txt";

			using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new VersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 10 });

				var aContent = "aaa";
				var bContent = "bbb";
				var cContent = "ccc";

				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(aContent));
				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(bContent));
				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(cContent));

				var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
				Assert.NotNull(stream);
				Assert.Equal(cContent, StreamToString(stream));

				stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
				Assert.NotNull(stream);
				Assert.Equal(aContent, StreamToString(stream));

				stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2");
				Assert.NotNull(stream);
				Assert.Equal(bContent, StreamToString(stream));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task MaxRevisions(string requestedStorage)
		{
			const string FileName = "file1.txt";

			using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new VersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, MaxRevisions = 3 });

				var aContent = "aaa";
				var bContent = "bbb";
				var cContent = "ccc";
				var dContent = "ddd";

				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(aContent));
				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(bContent));
				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(cContent));
				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(dContent));

				var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
				Assert.NotNull(stream);
				Assert.Equal(dContent, StreamToString(stream));

				await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1"));

				stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2");
				Assert.NotNull(stream);
				Assert.Equal(bContent, StreamToString(stream));

				stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/3");
				Assert.NotNull(stream);
				Assert.Equal(cContent, StreamToString(stream));
			}
		}
	}
}