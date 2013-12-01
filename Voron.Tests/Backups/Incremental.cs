// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using Voron.Impl;
using Voron.Impl.Backup;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Backups
{
	public class Incremental : StorageTest
	{
		private Func<int, string> _incrementalBackupFile = n => string.Format("voron-test.{0}-incremental-backup.zip", n);
		private const string _restoredStoragePath = "incremental-backup-test.data";

		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 1000 * AbstractPager.PageSize;
			options.IncrementalBackupEnabled = true;
			options.ManualFlushing = true;
		}

		public Incremental()
		{
			Clean();
		}

		[Fact]
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

			BackupMethods.Incremental.ToFile(Env, _incrementalBackupFile(0));

			var options = StorageEnvironmentOptions.ForPath(_restoredStoragePath);
			options.MaxLogFileSize = Env.Options.MaxLogFileSize;

			using (var env = new StorageEnvironment(options))
			{
				BackupMethods.Incremental.Restore(env, _incrementalBackupFile(0));

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					for (int i = 0; i < 500; i++)
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

		[Fact]
		public void CanDoMultipleIncrementalBackupsAndRestoreOneByOne()
		{
			var random = new Random();
			var buffer = new byte[1024];
			random.NextBytes(buffer);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 300; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				tx.Commit();
			}

			BackupMethods.Incremental.ToFile(Env, _incrementalBackupFile(0));

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 300; i < 600; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				tx.Commit();
			}

			BackupMethods.Incremental.ToFile(Env, _incrementalBackupFile(1));

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 600; i < 1000; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				tx.Commit();
			}

			Env.FlushLogToDataFile(); // make sure that incremental backup will work even if we flushed journals to the data file

			BackupMethods.Incremental.ToFile(Env, _incrementalBackupFile(2));

			var options = StorageEnvironmentOptions.ForPath(_restoredStoragePath);
			options.MaxLogFileSize = Env.Options.MaxLogFileSize;

			using (var env = new StorageEnvironment(options))
			{
				BackupMethods.Incremental.Restore(env, _incrementalBackupFile(0));
				BackupMethods.Incremental.Restore(env, _incrementalBackupFile(1));
				BackupMethods.Incremental.Restore(env, _incrementalBackupFile(2));

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

		[Fact]
		public void IncrementalBackupShouldCopyJustNewPagesSinceLastBackup()
		{
			var random = new Random();
			var buffer = new byte[100];
			random.NextBytes(buffer);

			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 0; i < 5; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				tx.Commit();
			}

		    var usedPagesInJournal = Env.Journal.CurrentFile.WritePagePosition;

			var backedUpPages = BackupMethods.Incremental.ToFile(Env, _incrementalBackupFile(0));

			Assert.Equal(usedPagesInJournal, backedUpPages);

			var writePos = Env.Journal.CurrentFile.WritePagePosition;
		
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 5; i < 10; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				tx.Commit();
			}

			var usedByLastTransaction = Env.Journal.CurrentFile.WritePagePosition - writePos;

			backedUpPages = BackupMethods.Incremental.ToFile(Env, _incrementalBackupFile(1));

			Assert.Equal(usedByLastTransaction, backedUpPages);

			var options = StorageEnvironmentOptions.ForPath(_restoredStoragePath);
			options.MaxLogFileSize = Env.Options.MaxLogFileSize;

			using (var env = new StorageEnvironment(options))
			{
				BackupMethods.Incremental.Restore(env, _incrementalBackupFile(0));
				BackupMethods.Incremental.Restore(env, _incrementalBackupFile(1));

				using (var tx = env.NewTransaction(TransactionFlags.Read))
				{
					for (int i = 0; i < 10; i++)
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
			foreach (var incBackupFile in Directory.EnumerateFiles(".", "*incremental-backup"))
			{
				File.Delete(incBackupFile);
			}

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