using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Counters;
using Raven.Abstractions.Data;
using Raven.Database.Config;
using Raven.Database.Counters;
using Raven.Database.Counters.Backup;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Tests.Helpers.Util;
using Xunit;

namespace Raven.Tests.Counters
{
	public class BackupRestoreTests : IDisposable
	{
		private readonly string BackupDestinationDirectory = "TestCounterBackup";
		private readonly string BackupSourceDirectory = "TestCounterData";
		private readonly string RestoreToDirectory = "TestCounterRestore";
		private readonly string DocumentDatabaseDirectory = "TestCounterDB";
		private const string CounterStorageId = "FooBar";

		private readonly CounterStorage storage;
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

			config.Counter.DataDirectory = BackupSourceDirectory;
			config.Settings["Raven/StorageTypeName"] = config.DefaultStorageTypeName;
			config.PostInit();

			storage = new CounterStorage("http://localhost:8080","TestCounter",config);
			storage.Environment.Options.IncrementalBackupEnabled = true;
		}

		private static void DeleteTempFolders()
		{
			var directoriesToDelete = Directory.EnumerateDirectories(Directory.GetCurrentDirectory(), "TestCounter*", SearchOption.TopDirectoryOnly).ToList();
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
		}

		[Fact]
		public void Full_backup_and_restore_should_work()
		{
			StoreCounterChange(5,storage);
			StoreCounterChange(-2, storage);
			StoreCounterChange(3, storage);

			var backupOperation = NewBackupOperation(false);
			backupOperation.Execute();

			var restoreOperation = NewRestoreOperation();
			restoreOperation.Execute();

			var restoreConfig = new RavenConfiguration
			{
				RunInMemory = false
			};
			restoreConfig.Counter.DataDirectory = RestoreToDirectory;

			using (var restoredStorage = new CounterStorage("http://localhost:8081", "RestoredCounter", restoreConfig))
			{
				using (var reader = restoredStorage.CreateReader())
				{
					Assert.Equal(6, reader.GetCounterTotal("Bar", "Foo"));
					/*var counter = reader.GetCounterValuesByPrefix("Bar", "Foo");
					var counterValues = counter.CounterValues.ToArray();

					Assert.Equal(8, counterValues[0].Value);
					Assert.True(counterValues[0].IsPositive());
					Assert.Equal(2, counterValues[1].Value);
					Assert.False(counterValues[1].IsPositive());*/
				}
			}
		}

		[Fact]
		public void Incremental_backup_and_restore_should_work()
		{
			var backupOperation = NewBackupOperation(true);

			StoreCounterChange(5, storage);
			backupOperation.Execute(); 
			Thread.Sleep(100);
			StoreCounterChange(-2, storage);
			backupOperation.Execute();
			Thread.Sleep(100);
			StoreCounterChange(3, storage);
			backupOperation.Execute();

			var restoreOperation = NewRestoreOperation();
			restoreOperation.Execute();

			var restoreConfig = new RavenConfiguration
			{
				RunInMemory = false
			};
			restoreConfig.Counter.DataDirectory = RestoreToDirectory;

			using (var restoredStorage = new CounterStorage("http://localhost:8081", "RestoredCounter", restoreConfig))
			{
				using (var reader = restoredStorage.CreateReader())
				{
					Assert.Equal(6, reader.GetCounterTotal("Bar", "Foo"));

					/*var counter = reader.GetCounterValuesByPrefix("Bar", "Foo");
					var counterValues = counter.CounterValues.ToArray();

					Assert.Equal(8, counterValues[0].Value);
					Assert.True(counterValues[0].IsPositive());
					Assert.Equal(2, counterValues[1].Value);
					Assert.False(counterValues[1].IsPositive());*/
				}
			}
		}

		private void StoreCounterChange(long change, CounterStorage counterStorage)
		{
			using (var writer = counterStorage.CreateWriter())
			{
				writer.Store("Bar", "Foo", change);
				writer.Commit();
			}
		}

		protected BackupOperation NewBackupOperation(bool isIncremental)
		{
			return new BackupOperation(storage,
				config.Counter.DataDirectory,
				BackupDestinationDirectory,
				storage.Environment,
				isIncremental,
				new CounterStorageDocument
				{
					Id = CounterStorageId
				});
		}

		protected RestoreOperation NewRestoreOperation()
		{			
			return new RestoreOperation(new CounterRestoreRequest
			{
				BackupLocation = BackupDestinationDirectory,
				Id = CounterStorageId,
				RestoreToLocation = RestoreToDirectory
			}, obj => { });
		}
	}
}
