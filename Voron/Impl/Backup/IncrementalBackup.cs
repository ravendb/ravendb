// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Backup
{
    public unsafe class IncrementalBackup
    {
        public long ToFile(StorageEnvironment env, string backupPath, CompressionLevel compression = CompressionLevel.Optimal)
        {
            if (env.Options.IncrementalBackupEnabled == false)
                throw new InvalidOperationException("Incremental backup is disabled for this storage");

            long numberOfBackedUpPages = 0;

            var copier = new DataCopier(AbstractPager.PageSize * 16);
            var backupSuccess = true;

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

                // txw.Commit(); intentionally not committing
            }

            using (env.NewTransaction(TransactionFlags.Read))
            {
                var usedJournals = new List<JournalFile>();

                try
                {
                    using (var file = new FileStream(backupPath, FileMode.Create))
                    using (var package = new ZipArchive(file, ZipArchiveMode.Create))
                    {
                        long lastBackedUpPage = -1;
                        long lastBackedUpFile = -1;

                        var firstJournalToBackup = backupInfo.LastBackedUpJournal;

                        if (firstJournalToBackup == -1)
                            firstJournalToBackup = 0; // first time that we do incremental backup

                        for (var journalNum = firstJournalToBackup; journalNum <= backupInfo.LastCreatedJournal; journalNum++)
                        {
                            var journalFile = env.Journal.Files.FirstOrDefault(x => x.Number == journalNum); // first check journal files currently being in use
                            if (journalFile == null)
                            {
                                long journalSize;
                                using (var pager = env.Options.OpenJournalPager(journalNum))
                                {
                                    journalSize = Utils.NearestPowerOfTwo(pager.NumberOfAllocatedPages * AbstractPager.PageSize);
                                    if (journalSize >= env.Options.MaxLogFileSize) // can't set for more than the max log file size
                                        throw new InvalidOperationException("Recovered journal size is " + journalSize +
                                                                            ", while the maximum journal size can be " + env.Options.MaxLogFileSize);
                                }

                                journalFile = new JournalFile(env.Options.CreateJournalWriter(journalNum, journalSize), journalNum);
                            }

                            journalFile.AddRef();

                            usedJournals.Add(journalFile);

                            var startBackupAt = 0L;
                            var pagesToCopy = journalFile.JournalWriter.NumberOfAllocatedPages;
                            if (journalFile.Number == backupInfo.LastBackedUpJournal)
                            {
                                startBackupAt = backupInfo.LastBackedUpJournalPage + 1;
                                pagesToCopy -= startBackupAt;
                            }

                            if (startBackupAt >= journalFile.JournalWriter.NumberOfAllocatedPages) // nothing to do here
                                continue;

                            var part = package.CreateEntry(StorageEnvironmentOptions.JournalName(journalNum), compression);
                            Debug.Assert(part != null);

                            if (journalFile.Number == lastWrittenLogFile)
                                pagesToCopy -= (journalFile.JournalWriter.NumberOfAllocatedPages - lastWrittenLogPage);

                            using (var stream = part.Open())
                            {
                                copier.ToStream(journalFile, startBackupAt, pagesToCopy, stream);
                            }

                            lastBackedUpFile = journalFile.Number;
                            if (journalFile.Number == backupInfo.LastCreatedJournal)
                            {
                                lastBackedUpPage = startBackupAt + pagesToCopy - 1;
                                // we used all of this file, so the next backup should start in the next file
                                if (lastBackedUpPage == (journalFile.JournalWriter.NumberOfAllocatedPages - 1))
                                {
                                    lastBackedUpPage = -1;
                                    lastBackedUpFile++;
                                }
                            }

                            numberOfBackedUpPages += pagesToCopy;
                        }

                        //Debug.Assert(lastBackedUpPage != -1);

                        env.HeaderAccessor.Modify(header =>
                            {
                                header->IncrementalBackup.LastBackedUpJournal = lastBackedUpFile;
                                header->IncrementalBackup.LastBackedUpJournalPage = lastBackedUpPage;
                            });
                    }
                }
                catch (Exception)
                {
                    backupSuccess = false;
                    throw;
                }
                finally
                {
                    foreach (var file in usedJournals)
                    {
                        if (backupSuccess) // if backup succeeded we can remove journals
                        {
                            if (file.Number != lastWrittenLogFile) // prevent deletion of the current journal
                            {
                                file.DeleteOnClose = true;
                            }
                        }

                        file.Release();
                    }
                }

                return numberOfBackedUpPages;
            }
        }

        public void Restore(StorageEnvironment env, string backupPath)
        {
            using (env.Journal.Applicator.TakeFlushingLock())
            using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
            {
                using (env.Options.AllowManualFlushing())
                {
                    env.FlushLogToDataFile(txw);
                }

                List<string> journalNames;

                using (var package = ZipFile.Open(backupPath, ZipArchiveMode.Read))
                {
                    journalNames = package.Entries.Select(x => x.Name).ToList();
                }

                var tempDir = Directory.CreateDirectory(Path.GetTempPath() + Guid.NewGuid()).FullName;
                var toDispose = new List<IDisposable>();

                try
                {
                    ZipFile.ExtractToDirectory(backupPath, tempDir);

                    TransactionHeader* lastTxHeader = null;

                    var pagesToWrite = new Dictionary<long, Func<Page>>();

                    foreach (var journalName in journalNames)
                    {
                        var pager = new Win32MemoryMapPager(Path.Combine(tempDir, journalName));
                        toDispose.Add(pager);

                        long number;

                        if (long.TryParse(journalName.Replace(".journal", string.Empty), out number) == false)
                        {
                            throw new InvalidOperationException("Cannot parse journal file number");
                        }

                        var recoveryPager = new Win32MemoryMapPager(Path.Combine(tempDir, StorageEnvironmentOptions.JournalRecoveryName(number)));
                        toDispose.Add(recoveryPager);

                        var reader = new JournalReader(pager, recoveryPager, 0, lastTxHeader);

                        while (reader.ReadOneTransaction(env.Options))
                        {
                            lastTxHeader = reader.LastTransactionHeader;
                        }

                        foreach (var translation in reader.TransactionPageTranslation)
                        {
                            var pageInJournal = translation.Value.JournalPos;
                            pagesToWrite[translation.Key] = () => recoveryPager.Read(pageInJournal);
                        }
                    }

                    var sortedPages = pagesToWrite.OrderBy(x => x.Key)
                                                  .Select(x => x.Value())
                                                  .ToList();

                    var last = sortedPages.Last();

                    env.Options.DataPager.EnsureContinuous(txw, last.PageNumber,
                                                    last.IsOverflow
                                                        ? env.Options.DataPager.GetNumberOfOverflowPages(
                                                            last.OverflowSize)
                                                        : 1);

                    foreach (var page in sortedPages)
                    {
                        env.Options.DataPager.Write(page);
                    }

                    env.Options.DataPager.Sync();

                    txw.State.Root = Tree.Open(txw, env._sliceComparer, &lastTxHeader->Root);
                    txw.State.FreeSpaceRoot = Tree.Open(txw, env._sliceComparer, &lastTxHeader->FreeSpace);

                    txw.State.FreeSpaceRoot.Name = Constants.FreeSpaceTreeName;
                    txw.State.Root.Name = Constants.RootTreeName;

                    txw.State.NextPageNumber = lastTxHeader->LastPageNumber + 1;

                    env.Journal.Clear(txw);

                    txw.Commit();
                }
                finally
                {
                    toDispose.ForEach(x => x.Dispose());

                    Directory.Delete(tempDir, true);
                }
            }
        }
    }
}