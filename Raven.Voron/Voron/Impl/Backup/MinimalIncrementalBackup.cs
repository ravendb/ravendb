using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Platform.Win32;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Backup
{
	public unsafe class MinimalIncrementalBackup
	{
		public void ToFile(StorageEnvironment env, string filename, CompressionLevel compression = CompressionLevel.Optimal,Action<string> infoNotify = null)
		{
			var pagesToWrite = new Dictionary<long, Page>();
			//todo: use the user's define temp path, instead
			var tempDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString())).FullName;
			if (infoNotify == null)
				infoNotify = str => { };
			var inMemoryRecoveryPagers = new List<Win32PageFileBackedMemoryMappedPager>();
			try
			{
				ImmutableAppendOnlyList<JournalFile> journalFiles;
				//todo: you are locking to get the journal files, but you are NOT getting the correct transaction header at the same time, 
				//todo: thereby opening yourself for a race condition. In particular, you may be reading a partial transaction from the 
				//todo: journal file, because you don't know when to stop
				using (env.NewTransaction(TransactionFlags.ReadWrite)) //tx here serves as "lock" 
					journalFiles = ImmutableAppendOnlyList<JournalFile>.CreateFrom(env.Journal.Files);
				
				infoNotify("Voron - reading storage journals for snapshot pages");
				//make sure we go over journal files in an ordered fashion
				foreach (var file in journalFiles.OrderBy(x => x.Number))
				{
					var recoveryPager = new Win32PageFileBackedMemoryMappedPager(StorageEnvironmentOptions.JournalRecoveryName(file.Number));
					inMemoryRecoveryPagers.Add(recoveryPager);

					using (var filePager = env.Options.OpenJournalPager(file.Number))
					{
						var reader = new JournalReader(filePager, recoveryPager, 0, null);
						while (reader.ReadOneTransaction(env.Options))
						{
							// read to end? 
						}

						foreach (var pagePosition in reader.TransactionPageTranslation)
						{
							var pageInJournal = pagePosition.Value.JournalPos;
							var page = recoveryPager.Read(pageInJournal);
							pagesToWrite[pagePosition.Key] = page;
							if (page.IsOverflow)
							{				
								var numberOfOverflowPages = recoveryPager.GetNumberOfOverflowPages(page.OverflowSize);
								for (int i = 1; i < numberOfOverflowPages; i++)
									pagesToWrite.Remove(pagePosition.Key + i);
							}
						}
					}
				}

				infoNotify("Voron - started writing snapshot file.");

				//todo: there is no need to do this, we can do it directly, we already have the data
				//todo: also, you aren't creating a merged transaction, also, you are going to be rejected on tx number as well.

				//using in-memory only StorageEnvironment allows to reproduce both AccessViolationException and
				//garbage in header of the exported tempEnv
				//using (var tempEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
				using (var tempEnv = new StorageEnvironment(StorageEnvironmentOptions.ForPath(tempDir)))
				{
					tempEnv.Options.IncrementalBackupEnabled = true;
					using (var tempTx = tempEnv.NewTransaction(TransactionFlags.ReadWrite))
					{
						var pages = pagesToWrite.Select(x => x.Value).ToArray();
						var maxPageNumber = pagesToWrite.Max(x => x.Value.PageNumber);
						tempTx.WriteDirect(pages, maxPageNumber + 1);
						tempTx.Commit();
					}

					var backup = new IncrementalBackup();
					backup.ToFile(tempEnv, filename, compression);
				}
			}
			finally
			{
				foreach (var pager in inMemoryRecoveryPagers)
					pager.Dispose();

				Directory.Delete(tempDir, true);
			}
		}
	}
}
