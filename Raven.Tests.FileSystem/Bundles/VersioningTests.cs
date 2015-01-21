// -----------------------------------------------------------------------
//  <copyright file="VersioningTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
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

		[Theory]
		[PropertyData("Storages")]
		public async Task WhenPurgeOnDeleteIsSetToFalseRevisionFilesShouldNotBeDeleted(string requestedStorage)
		{
			const string FileName = "file1.txt";

			using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new VersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, PurgeOnDelete = false });

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

				await store.AsyncFilesCommands.DeleteAsync(FileName);

				await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName));

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
		public async Task WhenPurgeOnDeleteIsSetToTrueRevisionFilesShouldBeDeleted(string requestedStorage)
		{
			const string FileName = "file1.txt";

			using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new VersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName, PurgeOnDelete = true });

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

				await store.AsyncFilesCommands.DeleteAsync(FileName);

				await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName));
				await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1"));
				await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/2"));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task RevisionsCannotBeDeletedWithoutProperSetting(string requestedStorage)
		{
			const string FileName = "file1.txt";

			using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new VersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

				var aContent = "aaa";
				var bContent = "bbb";

				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(aContent));
				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(bContent));

				var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
				Assert.NotNull(stream);
				Assert.Equal(bContent, StreamToString(stream));

				stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
				Assert.NotNull(stream);
				Assert.Equal(aContent, StreamToString(stream));

				var e = await AssertAsync.Throws<InvalidOperationException>(async () => await store.AsyncFilesCommands.DeleteAsync(FileName + "/revisions/1"));
				Assert.True(e.Message.Contains("Deleting a historical revision is not allowed"));

				stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
				Assert.NotNull(stream);
				Assert.Equal(aContent, StreamToString(stream));
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task RevisionsCanBeDeletedWithProperSetting(string requestedStorage)
		{
			const string FileName = "file1.txt";

			using (var store = NewStore(requestedStorage: requestedStorage, activeBundles: "Versioning", customConfig: configuration => configuration.Settings[Constants.FileSystem.Versioning.ChangesToRevisionsAllowed] = "true"))
			{
				await store.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new VersioningConfiguration { Id = VersioningUtil.DefaultConfigurationName });

				var aContent = "aaa";
				var bContent = "bbb";

				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(aContent));
				await store.AsyncFilesCommands.UploadAsync(FileName, StringToStream(bContent));

				var stream = await store.AsyncFilesCommands.DownloadAsync(FileName);
				Assert.NotNull(stream);
				Assert.Equal(bContent, StreamToString(stream));

				stream = await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1");
				Assert.NotNull(stream);
				Assert.Equal(aContent, StreamToString(stream));

				await store.AsyncFilesCommands.DeleteAsync(FileName + "/revisions/1");
				await AssertAsync.Throws<FileNotFoundException>(async () => await store.AsyncFilesCommands.DownloadAsync(FileName + "/revisions/1"));
			}
		}
	}
}