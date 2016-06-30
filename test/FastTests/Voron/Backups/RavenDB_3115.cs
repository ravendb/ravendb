// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3115.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using Xunit;
using Voron;
using Voron.Impl.Backup;

namespace FastTests.Voron.Backups
{
	public class RavenDB_3115 : StorageTest
	{
        private IncrementalBackupTestUtils IncrementalBackupTestUtils = new IncrementalBackupTestUtils();

        protected StorageEnvironmentOptions ModifyOptions(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 1000 * options.PageSize;
			options.IncrementalBackupEnabled = true;
			options.ManualFlushing = true;

			return options;
		}

		public RavenDB_3115()
		{
		    IncrementalBackupTestUtils.Clean();
		}

	    [Fact]
		public void ShouldCorrectlyLoadAfterRestartIfIncrementalBackupWasDone()
		{
			var bytes = new byte[1024];

			new Random().NextBytes(bytes);

			using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPath(DataDir)),NullLoggerSetup))
			{
				using (var tx = env.WriteTransaction())
				{
					tx.CreateTree(  "items");

					tx.Commit();
				}

				for (int j = 0; j < 100; j++)
				{
					using (var tx = env.WriteTransaction())
					{
						var tree = tx.ReadTree("items");

						for (int i = 0; i < 100; i++)
						{
							tree.Add("items/" + i, bytes);
						}

						tx.Commit();
					}
				}

				BackupMethods.Incremental.ToFile(env, IncrementalBackupTestUtils.IncrementalBackupFile(0));
			}

			// restart
			using (var env = new StorageEnvironment(ModifyOptions(StorageEnvironmentOptions.ForPath(DataDir)), NullLoggerSetup))
			{
			}
		}

		public override void Dispose()
		{
		    IncrementalBackupTestUtils.Clean();
		}
	}
}