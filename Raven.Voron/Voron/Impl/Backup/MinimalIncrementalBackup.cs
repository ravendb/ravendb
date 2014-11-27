using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Mono.Posix;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Platform.Posix;
using Voron.Platform.Win32;
using Voron.Trees;
using Voron.Util;
using FileMode = System.IO.FileMode;

namespace Voron.Impl.Backup
{
	public unsafe class MinimalIncrementalBackup
	{
		public void ToFile(StorageEnvironment env, string filename, CompressionLevel compression = CompressionLevel.Optimal,Action<string> infoNotify = null)
		{
			var pagesToWrite = new Dictionary<long, Page>();
			var tempDir = Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString())).FullName;
			if (infoNotify == null)
				infoNotify = str => { };
			var recoveryPagers = new List<Win32MemoryMapPager>();
			try
			{
				ImmutableAppendOnlyList<JournalFile> journalFiles;
				using (env.NewTransaction(TransactionFlags.ReadWrite)) //tx here serves as "lock" 
					journalFiles = env.Journal.Files;
				using (env.Journal.Applicator.TakeFlushingLock())
				{					
					infoNotify("Voron - reading storage journals for snapshot pages");
					foreach (var file in journalFiles)
					{
						var recoveryPager = new Win32MemoryMapPager(Path.Combine(tempDir, StorageEnvironmentOptions.JournalRecoveryName(file.Number)));
						recoveryPagers.Add(recoveryPager);
						using (var filePager = env.Options.OpenJournalPager(file.Number))
						{
							TransactionHeader* lastTxHeader;
							var journalReader = ReadJournalFile(env, filePager, recoveryPager, out lastTxHeader);

							foreach (var pagePosition in journalReader.TransactionPageTranslation)
							{
								int totalPageCount;
								ReadPageToWrite(pagePosition, recoveryPager, pagesToWrite, out totalPageCount);
							}
						}
					}
				}

				infoNotify("Voron - started writing snapshot file.");
				using (var tempEnv = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
				{
					tempEnv.Options.IncrementalBackupEnabled = true;
					using (tempEnv.Journal.Applicator.TakeFlushingLock())
					{
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

			}
			finally
			{
				foreach (var pager in recoveryPagers)
					pager.Dispose();

				Directory.Delete(tempDir, true);
			}
		}

		private static int ReadPageToWrite(KeyValuePair<long, JournalReader.RecoveryPagePosition> translation, Win32MemoryMapPager recoveryPager, Dictionary<long, Page> pagesToWrite, out int totalPageCount)
		{
			int size = 0;
			totalPageCount = 1;
			var pageInJournal = translation.Value.JournalPos;
			var page = recoveryPager.Read(pageInJournal);
			pagesToWrite[translation.Key] = page;
			size += AbstractPager.PageSize;
			if (page.IsOverflow)
			{				
				var numberOfOverflowPages = recoveryPager.GetNumberOfOverflowPages(page.OverflowSize);
				var overflowPagesCount = numberOfOverflowPages - 1;
				size += (AbstractPager.PageSize * overflowPagesCount);
				totalPageCount += overflowPagesCount;
				for (int i = 1; i < numberOfOverflowPages; i++)
					pagesToWrite.Remove(translation.Key + i);
			}

			return size;
		}

		private static JournalReader ReadJournalFile(StorageEnvironment env, IVirtualPager filePager, IVirtualPager recoveryPager,out TransactionHeader* lastTxHeader)
		{
			lastTxHeader = null;
			var reader = new JournalReader(filePager, recoveryPager, 0, null);
			while (reader.ReadOneTransaction(env.Options))
				lastTxHeader = reader.LastTransactionHeader;
			return reader;
		}
	}
}
