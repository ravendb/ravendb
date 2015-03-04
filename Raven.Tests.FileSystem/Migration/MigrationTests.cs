// -----------------------------------------------------------------------
//  <copyright file="BackupCreator.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.FileSystem.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.FileSystem.Migration
{
	public class MigrationTests : RavenFilesTestBase
	{
		[Theory]
		[PropertyData("Storages")]
		public async Task CreateDataToMigrate(string storage)
		{
			string build = "3528"; // CHANGE THIS IF YOU CREATE A NEW BACKUP

			var source = NewAsyncClient(0, requestedStorage: storage, runInMemory: false);
			var destination = NewAsyncClient(1, requestedStorage: storage, runInMemory: false);

			await source.Synchronization.SetDestinationsAsync(destination.ToSynchronizationDestination());

			for (int i = 0; i < 10; i++)
			{
				await source.UploadAsync(SynchronizedFileName(i), CreateUniformFileStream(i, (char) i));
			}

			SpinWait.SpinUntil(() => destination.GetStatisticsAsync().Result.FileCount == 10, TimeSpan.FromMinutes(1));

			var destinationStats = await destination.GetStatisticsAsync();

			Assert.Equal(10, destinationStats.FileCount);

			for (int i = 0; i < 10; i++)
			{
				await destination.UploadAsync(FileName(i), CreateUniformFileStream(i, (char) i));
			}

			for (int i = 0; i < 10; i++)
			{
				await destination.Configuration.SetKeyAsync(ConfigurationName(i), new RavenJObject(){ {"key", string.Format("value-{0}", i)}});
			}

			await source.Admin.StartBackup(string.Format("source-{0}-{1}", build, storage), null, false, source.FileSystem);
			WaitForBackup(source, true);

			await destination.Admin.StartBackup(string.Format("destination-{0}-{1}", build, storage), null, false, source.FileSystem);
			WaitForBackup(destination, true);
		}

		private static string SynchronizedFileName(int i)
		{
			return string.Format("source.{0}.file", i);
		}

		private static string FileName(int i)
		{
			return string.Format("{0}.file", i);
		}

		private static string ConfigurationName(int i)
		{
			return string.Format("Configurations/{0}", i);
		}
	}
}