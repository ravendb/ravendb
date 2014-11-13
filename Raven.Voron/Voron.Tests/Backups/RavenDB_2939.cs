// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2939.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;
using Voron.Impl.Backup;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Backups
{
	public class RavenDB_2939 : StorageTest
	{
		private Func<int, string> _incrementalBackupFile = n => string.Format("voron-test.{0}-incremental-backup.zip", n);
		private const string _restoredStoragePath = "incremental-backup-test.data";

		public RavenDB_2939()
		{
			Clean();
		}

		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 1000 * AbstractPager.PageSize;
			options.ManualFlushing = true;
		}

		[Fact]
		public void ShouldExplicitlyErrorThatTurningOnIncrementalBackupAfterInitializingTheStorageIsntAllowed()
		{
			RequireFileBasedPager();

			var random = new Random();
			var buffer = new byte[4000];
			random.NextBytes(buffer);

			for (int i = 0; i < 300; i++)
			{
				using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
				{
					tx.State.Root.Add("items/" + i, new MemoryStream(buffer));
					tx.Commit();
				}
			}

			Env.FlushLogToDataFile();

			Env.Options.IncrementalBackupEnabled = true;

			var exception = Assert.Throws<InvalidOperationException>(() => BackupMethods.Incremental.ToFile(Env, _incrementalBackupFile(0)));

			Assert.Equal("The first incremental backup creation failed because the first journal file " + StorageEnvironmentOptions.JournalName(0) + " was not found. Did you turn on the incremental backup feature after initializing the storage? In order to create backups incrementally the storage must be created with IncrementalBackupEnabled option set to 'true'.", exception.Message);
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