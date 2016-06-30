using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Xunit;
using Voron;
using Voron.Impl.Backup;

namespace FastTests.Voron.Backups
{
	public class MinimalIncrementalBackupTests : StorageTest
	{
		protected override void Configure(StorageEnvironmentOptions options)
		{
			options.IncrementalBackupEnabled = true;
			options.MaxLogFileSize = 1000 * options.PageSize;
		}

	    [Fact]
		public void Can_write_minimal_incremental_backup_and_restore_with_regular_incremental()
		{
			const int UserCount = 5000;
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions, NullLoggerSetup))
			{
				int index = 0;
				for (int xi = 0; xi < 10; xi++)
				{
					using (var tx = envToSnapshot.WriteTransaction())
					{
						var tree = tx.CreateTree("test");

						for (int i = 0; i < UserCount / 10; i++)
						{
							tree.Add("users/" + index, "john doe/" + index);
							index++;
						}

						tx.Commit();
					}
				}

				var snapshotWriter = new MinimalIncrementalBackup();
				snapshotWriter.ToFile(envToSnapshot, Path.Combine(DataDir, "1.snapshot"));
			}

			var incremental = new IncrementalBackup();

			var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "restored"));
			incremental.Restore(restoredOptions, new[] { Path.Combine(DataDir, "1.snapshot") }, NullLoggerSetup);

			using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions, NullLoggerSetup))
			{
				using (var tx = snapshotRestoreEnv.ReadTransaction())
				{
					var tree = tx.ReadTree("test");
					Assert.NotNull(tree);

					for (int i = 0; i < UserCount; i++)
					{
						var readResult = tree.Read("users/" + i);
						Assert.NotNull(readResult);
						Assert.Equal("john doe/" + i, readResult.Reader.ToStringValue());
					}
				}
			}
		}

		[Fact]
		public void Can_use_full_back_then_full_min_backup()
		{
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions, NullLoggerSetup))
			{
				using (var tx = envToSnapshot.WriteTransaction())
				{
					var tree = tx.CreateTree("test");

					for (int i = 0; i < 1000; i++)
					{
						tree.Add("users/" + i, "first/" + i);
					}

					tx.Commit();
				}

				new FullBackup().ToFile(envToSnapshot, Path.Combine(DataDir, "full.backup"));

				using (var tx = envToSnapshot.WriteTransaction())
				{
					var tree = tx.CreateTree( "test");

					for (int i = 0; i < 500; i++)
					{
						tree.Add("users/" + i, "second/" + (i * 2));
					}

					for (int i = 0; i < 500; i++)
					{
						tree.Add("users/" + (i + 10000), "third/" + i);
					}

					tx.Commit();
				}
				new MinimalIncrementalBackup().ToFile(envToSnapshot, Path.Combine(DataDir, "1.backup"));
			}


			new FullBackup().Restore(Path.Combine(DataDir, "full.backup"), Path.Combine(DataDir, "restored"));
			var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "restored"));
			new IncrementalBackup().Restore(restoredOptions, new[] { Path.Combine(DataDir, "1.backup") }, NullLoggerSetup);

			using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions, NullLoggerSetup))
			{
				using (var tx = snapshotRestoreEnv.ReadTransaction())
				{
					var tree = tx.ReadTree("test");
					Assert.NotNull(tree);

					for (int i = 0; i < 500; i++)
					{
						var readResult = tree.Read("users/" + i);
						Assert.NotNull(readResult);
						Assert.Equal("second/" + (i * 2), readResult.Reader.ToStringValue());
					}
					for (int i = 0; i < 500; i++)
					{
						var readResult = tree.Read("users/" + (i + 10000));
						Assert.NotNull(readResult);
						Assert.Equal("third/" + i, readResult.Reader.ToStringValue());
					}

					for (int i = 0; i < 500; i++)
					{
						var readResult = tree.Read("users/" + (i + 500));
						Assert.NotNull(readResult);
						Assert.Equal("first/" + (i+500), readResult.Reader.ToStringValue());
					}
				}
			}
		}

		[Fact]
		public void Can_make_multiple_min_inc_backups_and_then_restore()
		{
			const int UserCount = 5000;
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions, NullLoggerSetup))
			{
				int index = 0;
				for (int xi = 0; xi < 5; xi++)
				{
					for (int yi = 0; yi < 2; yi++)
					{
						using (var tx = envToSnapshot.WriteTransaction())
						{
							var tree = tx.CreateTree( "test");

							for (int i = 0; i < UserCount / 10; i++)
							{
								tree.Add("users/" + index, "john doe/" + index);
								index++;
							}

							tx.Commit();
						}
					}
					var snapshotWriter = new MinimalIncrementalBackup();
					snapshotWriter.ToFile(envToSnapshot, Path.Combine(DataDir, xi + ".snapshot"));
				}
			}

			var incremental = new IncrementalBackup();

			var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "restored"));
			incremental.Restore(restoredOptions, Enumerable.Range(0, 5).Select(i => Path.Combine(DataDir, i + ".snapshot")), NullLoggerSetup);

			using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions, NullLoggerSetup))
			{
				using (var tx = snapshotRestoreEnv.ReadTransaction())
				{
					var tree = tx.ReadTree("test");
					Assert.NotNull(tree);

					for (int i = 0; i < UserCount; i++)
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
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions, NullLoggerSetup))
			{
				for (int xi = 0; xi < 10; xi++)
				{
					using (var tx = envToSnapshot.WriteTransaction())
					{
						var tree = tx.CreateTree( "test");

						for (int i = 0; i < UserCount / 10; i++)
						{
							tree.Add("users/" + i, "john doe/" + i);
						}

						tx.Commit();
					}
				}

				var incrementalBackupInfo = envToSnapshot.HeaderAccessor.Get(ptr => ptr->IncrementalBackup);

				var snapshotWriter = new MinimalIncrementalBackup();
				snapshotWriter.ToFile(envToSnapshot, Path.Combine(DataDir, "1.snapshot"));

				// reset the incremental backup stuff

				envToSnapshot.HeaderAccessor.Modify(ptr => ptr->IncrementalBackup = incrementalBackupInfo);

				var incBackup = new IncrementalBackup();
				incBackup.ToFile(envToSnapshot, Path.Combine(DataDir, "2.snapshot"));

				var incLen = new FileInfo(Path.Combine(DataDir, "2.snapshot")).Length;
				var minInLen = new FileInfo(Path.Combine(DataDir, "1.snapshot")).Length;

				Assert.True(incLen > minInLen);
			}
		}


		[Fact]
		public void Can_split_merged_transaction_to_multiple_tx()
		{
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			storageEnvironmentOptions.MaxNumberOfPagesInMergedTransaction = 8;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions, NullLoggerSetup))
			{
				for (int xi = 0; xi < 100; xi++)
				{
					using (var tx = envToSnapshot.WriteTransaction())
					{
						var tree = tx.CreateTree("test");

						for (int i = 0; i < 1000; i++)
						{
							tree.Add("users/" + i, "john doe/" + i);
						}

						tx.Commit();
					}
				}

				var snapshotWriter = new MinimalIncrementalBackup();
				var backupPath = Path.Combine(DataDir, "1.snapshot");
				snapshotWriter.ToFile(envToSnapshot, backupPath);

				using (var stream = File.OpenRead(backupPath))
				using (var zip = new ZipArchive(stream, ZipArchiveMode.Read))
				{
					Assert.True(zip.Entries.Count > 1);
				}
			}
		}

		[Fact]
		public void Mixed_small_and_overflow_changes()
		{
			var storageEnvironmentOptions = StorageEnvironmentOptions.ForPath(DataDir);
			storageEnvironmentOptions.IncrementalBackupEnabled = true;
			using (var envToSnapshot = new StorageEnvironment(storageEnvironmentOptions, NullLoggerSetup))
			{
				using (var tx = envToSnapshot.WriteTransaction())
				{
					var tree = tx.CreateTree("test");
					tree.Add("users/1", "john doe");
					tree.Add("users/2", new String('a', 5000));

					tx.Commit();
				}

				using (var tx = envToSnapshot.WriteTransaction())
				{
					var tree = tx.CreateTree("test");
					tree.Add("users/2", "jane darling");
					tree.Add("users/3", new String('b', 5000));

					tx.Commit();
				}

				var snapshotWriter = new MinimalIncrementalBackup();
				snapshotWriter.ToFile(envToSnapshot, Path.Combine(DataDir, "1.snapshot"));

				var restoredOptions = StorageEnvironmentOptions.ForPath(Path.Combine(DataDir, "restored"));
				new IncrementalBackup().Restore(restoredOptions, new[] { Path.Combine(DataDir, "1.snapshot") }, NullLoggerSetup);

				using (var snapshotRestoreEnv = new StorageEnvironment(restoredOptions, NullLoggerSetup))
				{
					using (var tx = snapshotRestoreEnv.ReadTransaction())
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

		[Fact]
		public void Can_write_minimal_incremental_backup()
		{
			Directory.CreateDirectory(DataDir);

			var snapshotWriter = new MinimalIncrementalBackup();
			snapshotWriter.ToFile(Env, Path.Combine(DataDir, "1.snapshot"));

			Assert.True(File.Exists(Path.Combine(DataDir, "1.snapshot")), " Even empty minimal backup should create a file");

			var snapshotFileInfo = new FileInfo(Path.Combine(DataDir, "1.snapshot"));
			Assert.True(snapshotFileInfo.Length > 0, " Even empty minimal backup should create a file with some information");
		}
	}
}
