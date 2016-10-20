// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2939.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using SlowTests.Utils;
using Voron;
using Voron.Impl.Backup;
using Xunit;

namespace SlowTests.Voron
{
	public class RavenDB_2939 : StorageTest
	{
	    private IncrementalBackupTestUtils IncrementalBackupTestUtils = new IncrementalBackupTestUtils();

        public RavenDB_2939()
		{
			IncrementalBackupTestUtils.Clean();
		}

		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 1000 * options.PageSize;
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
				using (var tx = Env.WriteTransaction())
				{
                    var tree = tx.CreateTree("foo");
                    tree.Add("items/" + i, new MemoryStream(buffer));
					tx.Commit();
				}
			}

			Env.FlushLogToDataFile();
		    Env.Journal.Applicator.SyncDataFile();

            Env.Options.IncrementalBackupEnabled = true;

			var exception = Assert.Throws<InvalidOperationException>(() => BackupMethods.Incremental.ToFile(Env, IncrementalBackupTestUtils.IncrementalBackupFile(0)));

			Assert.Equal("The first incremental backup creation failed because the first journal file " + StorageEnvironmentOptions.JournalName(0) + " was not found. Did you turn on the incremental backup feature after initializing the storage? In order to create backups incrementally the storage must be created with IncrementalBackupEnabled option set to 'true'.", exception.Message);
		}

		public override void Dispose()
		{
			base.Dispose();
			IncrementalBackupTestUtils.Clean();
		}


	}
}