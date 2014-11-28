using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Mono.Unix.Native;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Platform.Win32;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Backup
{
	public unsafe class MinimalIncrementalBackup
	{
		public void ToFile(StorageEnvironment env, string backupPath, CompressionLevel compression = CompressionLevel.Optimal, Action<string> infoNotify = null,
			Action backupStarted = null)
		{
			var pageNumberToPageInScratch = new Dictionary<long, long>();
			if (infoNotify == null)
				infoNotify = str => { };
			var toDispose = new List<IDisposable>();
			try
			{
				IncrementalBackupInfo backupInfo;
				long lastWrittenLogPage = -1;
				long lastWrittenLogFile = -1;

				using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
				{
					backupInfo = env.HeaderAccessor.Get(ptr => ptr->IncrementalBackup);

					if (env.Journal.CurrentFile != null)
					{
						lastWrittenLogFile = env.Journal.CurrentFile.Number;
						lastWrittenLogPage = env.Journal.CurrentFile.WritePagePosition;
					}

					//txw.Commit(); - intentionally not committing
				}

				if (backupStarted != null)
					backupStarted();

				infoNotify("Voron - reading storage journals for snapshot pages");

				var lastBackedUpFile = backupInfo.LastBackedUpJournal;
				var lastBackedUpPage = backupInfo.LastBackedUpJournalPage;
				var firstJournalToBackup = backupInfo.LastBackedUpJournal;

				if (firstJournalToBackup == -1)
					firstJournalToBackup = 0; // first time that we do incremental backup

				var lastTransaction = new TransactionHeader { TransactionId = -1 };

				var recoveryPager = env.Options.CreateScratchPager("min-inc-backup.scratch");
				toDispose.Add(recoveryPager);
				int recoveryPage = 0;
				for (var journalNum = firstJournalToBackup; journalNum <= backupInfo.LastCreatedJournal; journalNum++)
				{
					lastBackedUpFile = journalNum;
					using (var journalFile = IncrementalBackup.GetJournalFile(env, journalNum, backupInfo))
					using (var filePager = env.Options.OpenJournalPager(journalNum))
					{
						var reader = new JournalReader(filePager, recoveryPager, 0, null, recoveryPage);
						reader.MaxPageToRead = lastBackedUpPage = journalFile.JournalWriter.NumberOfAllocatedPages;
						if (journalNum == lastWrittenLogFile) // set the last part of the log file we'll be reading
							reader.MaxPageToRead = lastBackedUpPage = lastWrittenLogPage;

						if (lastBackedUpPage == journalFile.JournalWriter.NumberOfAllocatedPages) // past the file size
						{
							// move to the next
							lastBackedUpPage = -1;
							lastBackedUpFile++;
						}

						if (journalNum == backupInfo.LastBackedUpJournal) // continue from last backup
							reader.SetStartPage(backupInfo.LastBackedUpJournalPage + 1);
						TransactionHeader* lastJournalTxHeader = null;
						while (reader.ReadOneTransaction(env.Options))
						{
							// read all transactions here 
							lastJournalTxHeader = reader.LastTransactionHeader;
						}

						if (lastJournalTxHeader != null)
							lastTransaction = *lastJournalTxHeader;

						recoveryPage = reader.RecoveryPage;

						foreach (var pagePosition in reader.TransactionPageTranslation)
						{
							var pageInJournal = pagePosition.Value.JournalPos;
							var page = recoveryPager.Read(pageInJournal);
							pageNumberToPageInScratch[pagePosition.Key] = pageInJournal;
							if (page.IsOverflow)
							{
								var numberOfOverflowPages = recoveryPager.GetNumberOfOverflowPages(page.OverflowSize);
								for (int i = 1; i < numberOfOverflowPages; i++)
									pageNumberToPageInScratch.Remove(pagePosition.Key + i);
							}
						}

					}
				}

				if (pageNumberToPageInScratch.Count == 0)
				{
					infoNotify("Voron - no changes since last backup, nothing to do");
					return;
				}

				infoNotify("Voron - started writing snapshot file.");

				if (lastTransaction.TransactionId == -1)
					throw new InvalidOperationException("Could not find any transactions in the journals, but found pages to write? That ain't right.");




				var compressionPager = env.Options.CreateScratchPager("min-inc-backup.compression-buffer");
				toDispose.Add(compressionPager);

				int totalNumberOfPages = 0;
				int start = 0;
				foreach (var pageNum in pageNumberToPageInScratch.Values)
				{
					var p = recoveryPager.Read(pageNum);
					var size = 1;
					if (p.IsOverflow)
					{
						size = recoveryPager.GetNumberOfOverflowPages(p.OverflowSize);
					}
					totalNumberOfPages += size;
					compressionPager.EnsureContinuous(null, start, size); //maybe increase size

					StdLib.memcpy(compressionPager.AcquirePagePointer(start), p.Base, size * AbstractPager.PageSize);

					start += size;
				}
				var txHeaderPage = start;
				compressionPager.EnsureContinuous(null, txHeaderPage, 1);//tx header
				start++;

				//TODO: what happens when we have enough transactions here that handle more than 4GB? 
				//TODO: in this case, we need to split this into multiple merged transactions, of up to 2GB 
				//TODO: each

				var uncompressedSize = totalNumberOfPages * AbstractPager.PageSize;
				var outputBufferSize = LZ4.MaximumOutputLength(uncompressedSize);

				compressionPager.EnsureContinuous(null, start,
					compressionPager.GetNumberOfOverflowPages(outputBufferSize));

				var txPage = compressionPager.GetWritable(txHeaderPage);
				StdLib.memset(txPage.Base, 0, AbstractPager.PageSize);
				var txHeader = (TransactionHeader*)txPage.Base;
				txHeader->HeaderMarker = Constants.TransactionHeaderMarker;

				txHeader->TransactionId = lastTransaction.TransactionId;
				txHeader->NextPageNumber = lastTransaction.NextPageNumber;
				txHeader->LastPageNumber = lastTransaction.LastPageNumber;
				txHeader->PageCount = totalNumberOfPages;
				txHeader->TxMarker = TransactionMarker.Commit | TransactionMarker.Merged;
				txHeader->Compressed = true;
				txHeader->UncompressedSize = uncompressedSize;

				using (var lz4 = new LZ4())
				{
					txHeader->CompressedSize = lz4.Encode64(compressionPager.AcquirePagePointer(0),
						compressionPager.AcquirePagePointer(start),
						txHeader->UncompressedSize,
						outputBufferSize);
				}

				txHeader->Crc = Crc.Value(compressionPager.AcquirePagePointer(start), 0, txHeader->CompressedSize);

				using (var file = new FileStream(backupPath, FileMode.Create))
				{
					using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
					{
						var entry = package.CreateEntry(string.Format("{0:D19}.journal", lastBackedUpFile));
						using (var stream = entry.Open())
						{
							var copier = new DataCopier(AbstractPager.PageSize * 16);
							copier.ToStream(compressionPager.AcquirePagePointer(txHeaderPage), (totalNumberOfPages + 1), stream);
						}
					}
					file.Flush(true);// make sure we hit the disk and stay there
				}

				env.HeaderAccessor.Modify(header =>
				{
					header->IncrementalBackup.LastBackedUpJournal = lastBackedUpFile;
					header->IncrementalBackup.LastBackedUpJournalPage = lastBackedUpPage;
				});
			}
			finally
			{
				foreach (var disposable in toDispose)
					disposable.Dispose();
			}
		}
	}
}
