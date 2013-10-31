// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron.Impl.Backup;
using Xunit;

namespace Voron.Tests.Backups
{
	public class Incremental : StorageTest
	{
		private const string _incrementalBackupFile = "voron-test.incremental-backup";
		private const string _restoredStoragePath = "incremental-backup-test.data";

		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 1000 * options.DataPager.PageSize;
			options.IncrementalBackupEnabled = true;
		}

		public Incremental()
		{
			Clean();
		}

		[Fact(Skip="Not implemented yet")]
		public void CanBackupAndRestoreOnEmptyStorage()
		{
			var random = new Random();
			var buffer = new byte[8192];
			random.NextBytes(buffer);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 500; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				tx.Commit();
			}

			BackupMethods.Incremental.ToFile(Env, _incrementalBackupFile);

			//TODO
			//using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			//{
			//	for (int i = 500; i < 1000; i++)
			//	{
			//		tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
			//	}

			//	tx.Commit();
			//}
			//BackupMethods.Incremental.ToFile(Env, _anotherincrementalBackupFile);

			var options = StorageEnvironmentOptions.ForPath(_restoredStoragePath);
			options.MaxLogFileSize = Env.Options.MaxLogFileSize;

			using (var env = new StorageEnvironment(options))
			{
				BackupMethods.Incremental.Restore(env, _incrementalBackupFile);

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					for (int i = 0; i < 1000; i++)
					{
						var readResult = tx.State.Root.Read(tx, "items/" + i);
						Assert.NotNull(readResult);
						var memoryStream = new MemoryStream();
						readResult.Stream.CopyTo(memoryStream);
						Assert.Equal(memoryStream.ToArray(), buffer);
					}
				}
			}
		}

		private void Clean()
		{
			if (File.Exists(_incrementalBackupFile))
				File.Delete(_incrementalBackupFile);

			if (Directory.Exists(_restoredStoragePath))
				Directory.Delete(_restoredStoragePath, true);
		}

		public override void Dispose()
		{
			base.Dispose();
			Clean();
		}	
	}
}