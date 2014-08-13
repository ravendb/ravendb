// -----------------------------------------------------------------------
//  <copyright file="AutomaticConflictResolutions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Replication;
using Raven.Client.FileSystem;
using Raven.Database.Server.RavenFS.Synchronization;
using Raven.Database.Server.RavenFS.Synchronization.Multipart;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;
using Xunit;

namespace RavenFS.Tests.Synchronization
{
	public class AutomaticConflictResolutions : RavenFsTestBase
	{
		[Fact]
		public async Task ShouldAutomaticallyResolveInFavourOfLocal()
		{
			var sourceClient = NewAsyncClient(0);
			var destinationClient = NewAsyncClient(1);

			await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
			{
				FileConflictResolution = StraightforwardConflictResolution.ResolveToLocal
			});

			var content = await ExecuteRawSynchronizationRequest(sourceClient, destinationClient);

			Assert.Equal("destination", content);
		}

		[Fact]
		public async Task ShouldAutomaticallyResolveInFavourOfRemote()
		{
			var sourceClient = NewAsyncClient(0);
			var destinationClient = NewAsyncClient(1);

			await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
			{
				FileConflictResolution = StraightforwardConflictResolution.ResolveToRemote
			});

			var content = await ExecuteRawSynchronizationRequest(sourceClient, destinationClient);

			Assert.Equal("source", content);
		}

		[Fact]
		public async Task ShouldAutomaticallyResolveInFavourOfLatest()
		{
			var sourceClient = NewAsyncClient(0);
			var destinationClient = NewAsyncClient(1);

			await destinationClient.Configuration.SetKeyAsync(SynchronizationConstants.RavenSynchronizationConfig, new SynchronizationConfig()
			{
				FileConflictResolution = StraightforwardConflictResolution.ResolveToLatest
			});

			var content = await ExecuteRawSynchronizationRequest(sourceClient, destinationClient, () => Thread.Sleep(1000));

			Assert.Equal("source", content);
		}

		private static async Task<string> ExecuteRawSynchronizationRequest(IAsyncFilesCommands sourceClient, IAsyncFilesCommands destinationClient, Action action = null)
		{
			await destinationClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("destination")));

			if (action != null)
				action();

			await sourceClient.UploadAsync("test", new MemoryStream(Encoding.UTF8.GetBytes("source")));

			var sourceStream = new MemoryStream();

			(await sourceClient.DownloadAsync("test")).CopyTo(sourceStream);

			var metadata = await sourceClient.GetMetadataForAsync("test");

			var request = new SynchronizationMultipartRequest(destinationClient.Synchronization, new ServerInfo()
			{
				FileSystemUrl = sourceClient.UrlFor(),
				Id = sourceClient.GetServerIdAsync().Result
			}, "test", metadata, sourceStream, new[]
			{
				new RdcNeed()
				{
					BlockLength = 6,
					BlockType = RdcNeedType.Source,
					FileOffset = 0
				}
			});

			var synchronizationReport = await request.PushChangesAsync(CancellationToken.None);

			Assert.Null(synchronizationReport.Exception);

			var stream = await destinationClient.DownloadAsync("test");

			return StreamToString(stream);
		}
	}
}