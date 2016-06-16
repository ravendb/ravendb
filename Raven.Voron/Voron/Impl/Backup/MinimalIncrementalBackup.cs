using Sparrow;
using Sparrow.Platform;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Backup
{
    public unsafe class MinimalIncrementalBackup
    {
        public void ToFile(StorageEnvironment env, string backupPath, CompressionLevel compression = CompressionLevel.Optimal, Action<string> infoNotify = null,
            Action backupStarted = null)
        {
            if (env.Options.IncrementalBackupEnabled == false)
                throw new InvalidOperationException("Incremental backup is disabled for this storage");

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
                    var journalFile = IncrementalBackup.GetJournalFile(env, journalNum, backupInfo);
                    try
                    {
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
                                reader.SetStartPage(backupInfo.LastBackedUpJournalPage);
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
                                var page = recoveryPager.Read(null, pageInJournal);
                                pageNumberToPageInScratch[pagePosition.Key] = pageInJournal;
                                if (page.IsOverflow)
                                {
                                    var numberOfOverflowPages = recoveryPager.GetNumberOfOverflowPages(page.OverflowSize);
                                    for (int i = 1; i < numberOfOverflowPages; i++)
                                        pageNumberToPageInScratch.Remove(page.PageNumber + i);
                                }
                            }
                        }
                    }
                    finally
                    {
                        journalFile.Release();
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


                // it is possible that we merged enough transactions so the _merged_ output is too large for us.
                // Voron limit transactions to about 4GB each. That means that we can't just merge all transactions
                // blindly, for fear of hitting this limit. So we need to split things.
                // We are also limited to about 8 TB of data in general before we literally can't fit the number of pages into 
                // pageNumberToPageInScratch even theoretically.
                // We're fine with saying that you need to run min inc backup before you hit 8 TB in your increment, so that works for now.
                // We are also going to use env.Options.MaxScratchBufferSize to set the actual transaction limit here, to avoid issues 
                // down the road and to limit how big a single transaction can be before the theoretical 4GB limit.

                var nextJournalNum = lastBackedUpFile;
                using (var file = new FileStream(backupPath, FileMode.Create))
                {
                    using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                    {
                        var copier = new DataCopier(AbstractPager.PageSize * 16);

                        var finalPager = env.Options.CreateScratchPager("min-inc-backup-final.scratch");
                        toDispose.Add(finalPager);
                        finalPager.EnsureContinuous(null, 0, 1);//txHeader

                        foreach (var partition in Partition(pageNumberToPageInScratch.Values, env.Options.MaxNumberOfPagesInMergedTransaction))
                        {
                            int totalNumberOfPages = 0;
                            int overflowPages = 0;
                            int start = 1;
                            foreach (var pageNum in partition)
                            {
                                var p = recoveryPager.Read(null, pageNum);
                                var size = 1;
                                if (p.IsOverflow)
                                {
                                    size = recoveryPager.GetNumberOfOverflowPages(p.OverflowSize);
                                    overflowPages += (size - 1);
                                }
                                totalNumberOfPages += size;
                                finalPager.EnsureContinuous(null, start, size); //maybe increase size

                                Memory.Copy(finalPager.AcquirePagePointer(null, start), p.Base, size * AbstractPager.PageSize);

                                start += size;
                            }


                            var txPage = finalPager.AcquirePagePointer(null, 0);
                            UnmanagedMemory.Set(txPage, 0, AbstractPager.PageSize);
                            var txHeader = (TransactionHeader*)txPage;
                            txHeader->HeaderMarker = Constants.TransactionHeaderMarker;
                            txHeader->FreeSpace = lastTransaction.FreeSpace;
                            txHeader->Root = lastTransaction.Root;
                            txHeader->OverflowPageCount = overflowPages;
                            txHeader->PageCount = totalNumberOfPages - overflowPages;
                            txHeader->PreviousTransactionCrc = lastTransaction.PreviousTransactionCrc;
                            txHeader->TransactionId = lastTransaction.TransactionId;
                            txHeader->NextPageNumber = lastTransaction.NextPageNumber;
                            txHeader->LastPageNumber = lastTransaction.LastPageNumber;
                            txHeader->TxMarker = TransactionMarker.Commit | TransactionMarker.Merged;
                            txHeader->Compressed = false;
                            txHeader->UncompressedSize = txHeader->CompressedSize = totalNumberOfPages * AbstractPager.PageSize;

                            txHeader->Crc = Crc.Value(finalPager.AcquirePagePointer(null, 1), 0, totalNumberOfPages * AbstractPager.PageSize);


                            var entry = package.CreateEntry(string.Format("{0:D19}.merged-journal", nextJournalNum), compression);
                            nextJournalNum++;
                            using (var stream = entry.Open())
                            {
                                copier.ToStream(finalPager.AcquirePagePointer(null, 0), (totalNumberOfPages + 1) * AbstractPager.PageSize, stream, CancellationToken.None);
                            }
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

        private IEnumerable<IEnumerable<long>> Partition(IEnumerable<long> src, long max)
        {
            using(var enumerator = src.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    yield return Partition(enumerator, max);
                }
            }
        }

        private IEnumerable<long> Partition(IEnumerator<long> src, long max)
        {
            do
            {
                yield return src.Current;
            } while (src.MoveNext() && max-- > 0);
        } 
    }
}
