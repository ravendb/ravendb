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
		private const string IncBackupFilename = "data.inc-backup";

		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.IncrementalBackupEnabled = true;
			options.MaxLogFileSize = 1000 * AbstractPager.PageSize;		
		}

		public override void Dispose()
		{
			base.Dispose();

			File.Delete(SnapshotFilename);
			File.Delete(IncBackupFilename);
			if(_tempDir != null)
				Directory.Delete(_tempDir, true);			
		}

		[Fact]
		public void Can_write_minimal_incremental_backup_and_restore_with_regular_incremental()
		{
			const int UserCount = 5000;
			_tempDir = Guid.NewGuid().ToString();
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
			{
				int index = 0;
				for (int xi = 0; xi < 10; xi++)
				{
					using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
					{
						var tree = envToSnapshot.CreateTree(tx, "test");

						for (int i = 0; i < UserCount/10; i++)
						{
							tree.Add("users/" + index, "john doe/" + index);
							index++;
						}

						tx.Commit();
					}
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

					for(int i = 0; i<UserCount; i++)
					{
						var readResult = tree.Read("users/" + i);
						Assert.NotNull(readResult);
						Assert.Equal("john doe/" + i, readResult.Reader.ToStringValue());
					}
				}
			}
		}

		[Fact]
		public unsafe void Min_inc_backup_is_smaller_than_normal_inc_backup()
		{
			const int UserCount = 5000;
			_tempDir = Guid.NewGuid().ToString();
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
			{
				for (int xi = 0; xi < 10; xi++)
				{
					using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
					{
						var tree = envToSnapshot.CreateTree(tx, "test");

						for (int i = 0; i < UserCount/10; i++)
						{
							tree.Add("users/" + i, "john doe/" + i);
						}

						tx.Commit();
					}
				}

				var incrementalBackupInfo = envToSnapshot.HeaderAccessor.Get(ptr => ptr->IncrementalBackup);

				var snapshotWriter = new MinimalIncrementalBackup();
				snapshotWriter.ToFile(envToSnapshot, SnapshotFilename);

				// reset the incremental backup stuff

				envToSnapshot.HeaderAccessor.Modify(ptr => ptr->IncrementalBackup = incrementalBackupInfo);

				var incBackup = new IncrementalBackup();
				incBackup.ToFile(envToSnapshot, IncBackupFilename);

				var incLen = new FileInfo(IncBackupFilename).Length;
				var minInLen = new FileInfo(SnapshotFilename).Length;

				Assert.True(incLen > minInLen);
			}
		}

		[Fact]
		public void Mixed_small_and_overflow_changes()
		{
			const int UserCount = 5000;
			_tempDir = Guid.NewGuid().ToString();
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(_tempDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions))
			{
				using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = envToSnapshot.CreateTree(tx, "test");
					tree.Add("users/1", "john doe");
					tree.Add("users/2", new String('a', 5000));

					tx.Commit();
				}

				using (var tx = envToSnapshot.NewTransaction(TransactionFlags.ReadWrite))
				{
					var tree = envToSnapshot.CreateTree(tx, "test");
					tree.Add("users/2", "jane darling");
					tree.Add("users/3", new String('b', 5000));

					tx.Commit();
				}

				var snapshotWriter = new MinimalIncrementalBackup();
				snapshotWriter.ToFile(envToSnapshot, SnapshotFilename);

				var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(_tempDir, "restored"));
				new IncrementalBackup().Restore(restoredOptions, new[] { SnapshotFilename });

				using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions))
				{
					using (var tx = snapshotRestoreEnv.NewTransaction(TransactionFlags.Read))
					{
						var tree = tx.ReadTree("test");
						Assert.NotNull(tree);

						Assert.Equal("john doe", tree.Read("users/1").Reader.ToStringValue());
						Assert.Equal("jane darling", tree.Read("users/2").Reader.ToStringValue());
						Assert.Equal(new String('b', 5000), tree.Read("users/3").Reader.ToStringValue());
					}
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
