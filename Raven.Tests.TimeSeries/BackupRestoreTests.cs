/*
using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.TimeSeries;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.TimeSeries;
using Raven.Database.TimeSeries.Backup;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Tests.Helpers.Util;
using Xunit;

namespace Raven.Tests.TimeSeries
{
	public class BackupRestoreTests : IDisposable
	{
		private readonly string BackupDestinationDirectory = "TestTimeSeriesBackup";
		private readonly string BackupSourceDirectory = "TestTimeSeriesData";
		private readonly string RestoreToDirectory = "TestTimeSeriesRestore";
		private readonly string DocumentDatabaseDirectory = "TestTimeSeriesDB";
		private const string TimeSeriesId = "FooBar";

		private readonly TimeSeriesStorage storage;
		private readonly DocumentDatabase documentDatabase;
		private readonly RavenConfiguration config;

		public BackupRestoreTests()
		{
			DeleteTempFolders();

			var uniqueId = Guid.NewGuid();
			BackupDestinationDirectory += uniqueId;
			BackupSourceDirectory += uniqueId;
			RestoreToDirectory += uniqueId;
			DocumentDatabaseDirectory += uniqueId;

			config = new RavenConfiguration
			{
				Port = 8090,
				DataDirectory = DocumentDatabaseDirectory,
				RunInMemory = false,
				DefaultStorageTypeName = "Voron",
				AnonymousUserAccessMode = AnonymousUserAccessMode.Admin, 
				Encryption = { UseFips = SettingsHelper.UseFipsEncryptionAlgorithms },
			};

			config.TimeSeries.DataDirectory = BackupSourceDirectory;
			config.Settings["Raven/StorageTypeName"] = config.DefaultStorageTypeName;
			config.PostInit();

			storage = new TimeSeriesStorage("http://localhost:8080","TestTimeSeries",config);
			storage.TimeSeriesEnvironment.Options.IncrementalBackupEnabled = true;
			documentDatabase = new DocumentDatabase(config,null);
		}

		private static void DeleteTempFolders()
		{
			var directoriesToDelete = Directory.EnumerateDirectories(Directory.GetCurrentDirectory(), "TestTimeSeries*", SearchOption.TopDirectoryOnly).ToList();
			directoriesToDelete.ForEach(dir =>
			{
				try
				{
					IOExtensions.DeleteDirectory(dir);
				}
				catch (IOException)
				{
					//don't try to delete locked folders, they will be deleted next time
				}
			});
			
		}

		public void Dispose()
		{			
			storage.Dispose();
			documentDatabase.Dispose();
		}

		[Fact]
		public void Full_backup_and_restore_should_work()
		{
			StoreTimeSeriesChange(5,storage);
			StoreTimeSeriesChange(-2, storage);
			StoreTimeSeriesChange(3, storage);

			var backupOperation = NewBackupOperation(false);
			backupOperation.Execute();

			var restoreOperation = NewRestoreOperation();
			restoreOperation.Execute();

			var restoreConfig = new RavenConfiguration
			{
				RunInMemory = false
			};
			restoreConfig.TimeSeries.DataDirectory = RestoreToDirectory;

			using (var restoredStorage = new TimeSeriesStorage("http://localhost:8081", "RestoredTimeSeries", restoreConfig))
			{
				using (var reader = restoredStorage.CreateReader())
				{
					Assert.Equal(6,reader.GetTimeSeriesOverallTotal("Bar", "Foo"));
					var timeSeries = reader.GetTimeSeriesValuesByPrefix("Bar", "Foo");
					var timeSeriesValues = timeSeries.TimeSeriesValues.ToArray();

					Assert.Equal(8, timeSeriesValues[0].Value);
					Assert.True(timeSeriesValues[0].IsPositive());
					Assert.Equal(2, timeSeriesValues[1].Value);
					Assert.False(timeSeriesValues[1].IsPositive());
				}
			}
		}

		[Fact]
		public void Incremental_backup_and_restore_should_work()
		{
			var backupOperation = NewBackupOperation(true);

			StoreTimeSeriesChange(5, storage);
			backupOperation.Execute(); 
			Thread.Sleep(100);
			StoreTimeSeriesChange(-2, storage);
			backupOperation.Execute();
			Thread.Sleep(100);
			StoreTimeSeriesChange(3, storage);
			backupOperation.Execute();

			var restoreOperation = NewRestoreOperation();
			restoreOperation.Execute();

			var restoreConfig = new RavenConfiguration
			{
				RunInMemory = false
			};
			restoreConfig.TimeSeries.DataDirectory = RestoreToDirectory;

			using (var restoredStorage = new TimeSeriesStorage("http://localhost:8081", "RestoredTimeSeries", restoreConfig))
			{
				using (var reader = restoredStorage.CreateReader())
				{
					Assert.Equal(6, reader.GetTimeSeriesOverallTotal("Bar", "Foo"));

					var timeSeries = reader.GetTimeSeriesValuesByPrefix("Bar", "Foo");
					var timeSeriesValues = timeSeries.TimeSeriesValues.ToArray();

					Assert.Equal(8, timeSeriesValues[0].Value);
					Assert.True(timeSeriesValues[0].IsPositive());
					Assert.Equal(2, timeSeriesValues[1].Value);
					Assert.False(timeSeriesValues[1].IsPositive());
				}
			}
		}

		private void StoreTimeSeriesChange(long change, TimeSeriesStorage timeSeries)
		{
			using (var writer = timeSeries.CreateWriter(TODO))
			{
				writer.Store("Bar", "Foo", change);
				writer.Commit();
			}
		}

		protected BackupOperation NewBackupOperation(bool isIncremental)
		{
			return new BackupOperation(documentDatabase,
				config.TimeSeries.DataDirectory,
				BackupDestinationDirectory,
				storage.TimeSeriesEnvironment,
				isIncremental,
				new TimeSeriesDocument
				{
					Id = TimeSeriesId
				});
		}

		protected RestoreOperation NewRestoreOperation()
		{			
			return new RestoreOperation(new TimeSeriesRestoreRequest
			{
				BackupLocation = BackupDestinationDirectory,
				Id = TimeSeriesId,
				RestoreToLocation = RestoreToDirectory
			}, obj => { });
		}
	}
}
*/