using System;
using System.IO;
using System.IO.Packaging;
using Voron.Impl;
using Voron.Impl.Backup;
using Voron.Impl.Paging;
using Xunit;

namespace Voron.Tests.Backups
{
    public class Full : StorageTest
    {
	    private const string _backupFile = "voron-test.backup";
	    private const string _recoveredStoragePath = "backup-test.data";

	    protected override void Configure(StorageEnvironmentOptions options)
		{
			options.MaxLogFileSize = 1000 * AbstractPager.PageSize;
		    options.ManualFlushing = true;
		}

	    public Full()
	    {
		    DeleteBackupData();
	    }

	    private void DeleteBackupData()
	    {
		    if (File.Exists(_backupFile))
			    File.Delete(_backupFile);

		    if (Directory.Exists(_recoveredStoragePath))
			    Directory.Delete(_recoveredStoragePath, true);
	    }

	    [Fact]
        public void CanBackupAndRestore()
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

			Assert.True(Env.Journal.Files.Count > 1);

			Env.FlushLogToDataFile(); // force writing data to the data file
			 
			// add more data to journal files
			using (var tx = Env.NewTransaction(TransactionFlags.ReadWrite))
			{
				for (int i = 500; i < 1000; i++)
				{
					tx.State.Root.Add(tx, "items/" + i, new MemoryStream(buffer));
				}

				tx.Commit();
			}

			BackupMethods.Full.ToFile(Env, _backupFile);

			BackupMethods.Full.Restore(_backupFile, _recoveredStoragePath);

		    var options = StorageEnvironmentOptions.ForPath(_recoveredStoragePath);
		    options.MaxLogFileSize = Env.Options.MaxLogFileSize;

			using (var env = new StorageEnvironment(options))
			{
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

		public override void Dispose()
		{
			base.Dispose();
			DeleteBackupData();
		}	
    }
}