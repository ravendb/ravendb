// -----------------------------------------------------------------------
//  <copyright file="VersioningAndSynchronization.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.FileSystem;
using Raven.Client.FileSystem.Bundles.Versioning;
using Raven.Client.FileSystem.Extensions;
using Raven.Database.Bundles.Versioning.Data;
using Raven.Database.Extensions;
using Raven.Database.FileSystem.Bundles.Versioning;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Bundles.Versioning
{
	public class VersioningAndSynchronization : RavenFilesTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public async Task ContentUpdateSynchronizationShouldCreateRevisions(string requestedStorage)
		{
			using (var source = NewStore())
			using (var destination = NewStore(1, requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				const int maxRevisions = 2;

				await destination.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration
				{
					Id = VersioningUtil.DefaultConfigurationName,
					MaxRevisions = maxRevisions
				});

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(1024));
				var synchronizationReport = await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					Assert.NotNull(await dstSession.LoadFileAsync("file.txt"));

					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(1, revisions.Length);

					Assert.Equal((await dstSession.DownloadAsync("file.txt")).GetMD5Hash(), (await dstSession.DownloadAsync(revisions[0])).GetMD5Hash());
				}

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(2048));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					Assert.NotNull(await dstSession.LoadFileAsync("file.txt"));

					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(2, revisions.Length);

					Assert.Equal((await dstSession.DownloadAsync("file.txt")).GetMD5Hash(), (await dstSession.DownloadAsync(revisions[1])).GetMD5Hash());
				}

				await source.AsyncFilesCommands.UploadAsync("file.txt", StringToStream("123456789"));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					Assert.NotNull(await dstSession.LoadFileAsync("file.txt"));

					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(maxRevisions, revisions.Length); // cannot be more revisions than defined in configuration

					Assert.Equal((await dstSession.DownloadAsync("file.txt")).GetMD5Hash(), (await dstSession.DownloadAsync(revisions[1])).GetMD5Hash());
				}

				using (var dstSession = destination.OpenAsyncSession())
				{
					dstSession.RegisterUpload("file.txt", StringToStream("abc")); // make sure that you can upload a new version

					await dstSession.SaveChangesAsync();

					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(StreamToString(await dstSession.DownloadAsync(revisions[0])), "123456789");
					Assert.Equal(StreamToString(await dstSession.DownloadAsync(revisions[1])), "abc");
				}
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task MetadataUpdateSynchronizationShouldCreateRevisions(string requestedStorage)
		{
			using (var source = NewStore())
			using (var destination = NewStore(1, requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				const int maxRevisions = 2;

				await destination.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration
				{
					Id = VersioningUtil.DefaultConfigurationName,
					MaxRevisions = maxRevisions
				});

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(1024));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					Assert.NotNull(await dstSession.LoadFileAsync("file.txt"));

					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(1, revisions.Length);

					Assert.Equal((await dstSession.DownloadAsync("file.txt")).GetMD5Hash(), (await dstSession.DownloadAsync(revisions[0])).GetMD5Hash());
				}

				await source.AsyncFilesCommands.UpdateMetadataAsync("file.txt", new RavenJObject { { "New", "Metadata" } });
				var report = await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				Assert.Equal(SynchronizationType.MetadataUpdate, report.Type);

				using (var dstSession = destination.OpenAsyncSession())
				{
					Assert.NotNull(await dstSession.LoadFileAsync("file.txt"));

					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(2, revisions.Length);

					var metadata = (await dstSession.LoadFileAsync("file.txt")).Metadata;

					Assert.Equal("Metadata", metadata["New"]);
				}

				await source.AsyncFilesCommands.UpdateMetadataAsync("file.txt", new RavenJObject { { "Another", "Metadata" } });
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					Assert.NotNull(await dstSession.LoadFileAsync("file.txt"));

					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(maxRevisions, revisions.Length); // cannot be more revisions than defined in configuration

					var metadata = (await dstSession.LoadFileAsync("file.txt")).Metadata;

					Assert.Equal("Metadata", metadata["Another"]);
				}

				using (var dstSession = destination.OpenAsyncSession())
				{
					var file = await dstSession.LoadFileAsync("file.txt");

					file.Metadata.Add("Test", "Test");

					await dstSession.SaveChangesAsync();

					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal("Metadata", (await dstSession.LoadFileAsync(revisions[0])).Metadata["Another"]);
					Assert.Equal("Test", (await dstSession.LoadFileAsync(revisions[1])).Metadata["Test"]);
				}
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task RenameSynchronizationShouldRemoveRevisionsByDefault(string requestedStorage)
		{
			using (var source = NewStore())
			using (var destination = NewStore(1, requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				await destination.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration
				{
					Id = VersioningUtil.DefaultConfigurationName,
				});

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(1024));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(1, revisions.Length);
				}

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(1024,  'c'));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(2, revisions.Length);
				}

				await source.AsyncFilesCommands.RenameAsync("file.txt", "renamed.txt");
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("renamed.txt", 0, 128);

					Assert.Equal(1, revisions.Length);
				}

			}
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task RenameSynchronizationShouldKeepRevisionsIfDefined(string requestedStorage)
		{
			using (var source = NewStore())
			using (var destination = NewStore(1, requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				await destination.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration
				{
					Id = VersioningUtil.DefaultConfigurationName,
					ResetOnRename = false
				});

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(1024));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(1, revisions.Length);
				}

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(1024, 'c'));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(2, revisions.Length);
				}

				await source.AsyncFilesCommands.RenameAsync("file.txt", "renamed.txt");
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("renamed.txt", 0, 128);

					Assert.Equal(2, revisions.Length);
				}
			}
		}

		[Theory]
		[PropertyData("Storages")]
		public async Task DeleteSynchronizationShouldDeleteRevisions(string requestedStorage)
		{
			using (var source = NewStore())
			using (var destination = NewStore(1, requestedStorage: requestedStorage, activeBundles: "Versioning"))
			{
				await destination.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration
				{
					Id = VersioningUtil.DefaultConfigurationName,
					ResetOnRename = false
				});

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(1024));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(1, revisions.Length);
				}

				await source.AsyncFilesCommands.UploadAsync("file.txt", CreateUniformFileStream(1024, 'c'));
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("file.txt", 0, 128);

					Assert.Equal(2, revisions.Length);
				}

				await source.AsyncFilesCommands.DeleteAsync("file.txt");
				await source.AsyncFilesCommands.Synchronization.StartAsync("file.txt", destination.AsyncFilesCommands);

				using (var dstSession = destination.OpenAsyncSession())
				{
					var revisions = await dstSession.GetRevisionNamesForAsync("renamed.txt", 0, 128);

					Assert.Equal(0, revisions.Length);
				}
			}
		}

		[Fact]
		public async Task RevisionsShouldNotBeSynchronized()
		{
			using (var source = NewStore(activeBundles: "Versioning"))
			using (var destination = NewStore(1))
			{
				const int maxRevisions = 2;

				await source.AsyncFilesCommands.Configuration.SetKeyAsync(VersioningUtil.DefaultConfigurationName, new FileVersioningConfiguration
				{
					Id = VersioningUtil.DefaultConfigurationName,
					MaxRevisions = maxRevisions
				});

				await source.AsyncFilesCommands.UploadAsync("test.txt", StringToStream("abc"));
				await source.AsyncFilesCommands.UploadAsync("test.txt", StringToStream("def"));

				await source.AsyncFilesCommands.Synchronization.SetDestinationsAsync(destination.AsyncFilesCommands.ToSynchronizationDestination());

				var results = await source.AsyncFilesCommands.Synchronization.StartAsync(true);
			
				Assert.Equal(1, results.Length);
				Assert.Equal(1, results[0].Reports.Count());

				using (var dstSession = destination.OpenAsyncSession())
				{
					Assert.NotNull(await dstSession.LoadFileAsync("test.txt"));

					var revisions = await dstSession.GetRevisionNamesForAsync("test.txt", 0, 128);

					Assert.Equal(0, revisions.Length);
				}
			}
		}
	}
}