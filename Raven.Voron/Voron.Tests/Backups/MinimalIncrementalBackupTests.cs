using System;
using System.IO;
using System.Text;
using Voron.Impl.Backup;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Backups
{
	public class MinimalIncrementalBackupTests : StorageTest
	{
		private string _tempDir;
		private const string SnapshotFilename = "data.snapshot";

		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 1000 * AbstractPager.PageSize;		
		}

		public override void Dispose()
		{
			base.Dispose();

			File.Delete(SnapshotFilename);
			if(_tempDir != null)
				Directory.Delete(_tempDir, true);			
		}

		[Fact]
		public void Can_write_minimal_incremental_backup_and_restore_with_regular_incremental()
		{
			const int UserCount = 5000;
			_tempDir = Guid.NewGuid().ToString();
			using (var envToSnapshot = new StorageEnvironment(StorageEnvironmentOptions.ForPath(_tempDir)))
			{
				using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = envToSnapshot.CreateTree(tx, "test");

					for(int i = 1; i<UserCount; i++)
						tree.Add("users/" + i, "john doe/" + i);

					tx.Commit();					
				}

				var snapshotWriter = new MinimalIncrementalBackup();
				snapshotWriter.ToFile(envToSnapshot, SnapshotFilename);
			}

			var incremental = new IncrementalBackup();

			var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(_tempDir, "restored"));
			incremental.Restore(restoredOptions, new[] { SnapshotFilename });

			using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions))
			{
				using (var tx = snapshotRestoreEnv.NewTransaction(TransactionFlags.Read))
				{
					var tree = tx.ReadTree("test");
					Assert.NotNull(tree);

					for(int i = 1; i<UserCount; i++)
						Assert.Equal("john doe/" + i, tree.Read("users/" + i).Reader.ToStringValue());
				}
			}
		}

		//sanity check
		[Fact]
		public void Can_write_minimal_incremental_backup()
		{
			try
			{
				var snapshotWriter = new MinimalIncrementalBackup();
				snapshotWriter.ToFile(Env, SnapshotFilename);

				Assert.True(File.Exists(SnapshotFilename), " Even empty minimal backup should create a file");

				var snapshotFileInfo = new FileInfo(SnapshotFilename);
				Assert.True(snapshotFileInfo.Length > 0, " Even empty minimal backup should create a file with some information");
			}
			finally
			{
				File.Delete(SnapshotFilename);
			}
		}
	}
}
