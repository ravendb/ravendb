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
        public class IncrementalRestorePaths
        {
            private string _journalLocation;
            public string DatabaseLocation { get; set; }
            public string JournalLocation
            {
                get { return _journalLocation ?? DatabaseLocation; }
                set { _journalLocation = value; }
            }
        }

        public long ToFile(StorageEnvironment env, string backupPath, CompressionLevel compression = CompressionLevel.Optimal)
        {
            if (env.Options.IncrementalBackupEnabled == false)
                throw new InvalidOperationException("Incremental backup is disabled for this storage");

            long numberOfBackedUpPages = 0;

            var copier = new DataCopier(AbstractPager.PageSize * 16);
            var backupSuccess = true;

            long lastWrittenLogPage = -1;
            long lastWrittenLogFile = -1;

            using (var file = new FileStream(backupPath, FileMode.Create))
            using (var package = new ZipArchive(file, ZipArchiveMode.Create))
            {
                IncrementalBackupInfo backupInfo;
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
                    catch (Exception)
                    {
                        backupSuccess = false;
                        throw;
                    }
                    finally
                    {
                        foreach (var jrnl in usedJournals)
                        {
                            if (backupSuccess) // if backup succeeded we can remove journals
                            {
                                if (jrnl.Number != lastWrittenLogFile) // prevent deletion of the current journal
                                {
                                    jrnl.DeleteOnClose = true;
                                }
                            }

                            jrnl.Release();
                        }
                    }

                    return numberOfBackedUpPages;
                }
            }
        }

        public void Restore(StorageEnvironmentOptions options, IEnumerable<string> backupPaths)
        {
            var ownsPagers = options.OwnsPagers;
            options.OwnsPagers = false;
            using (var env = new StorageEnvironment(options))
            {
                foreach (var backupPath in backupPaths)
                {
                    Restore(env, backupPath);
                }
            }
            options.OwnsPagers = ownsPagers;
        }

        private void Restore(StorageEnvironment env, string singleBackupFile)
        {
            using (env.Journal.Applicator.TakeFlushingLock())
            {
                using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    using (env.Options.AllowManualFlushing())
                    {
                        env.FlushLogToDataFile(txw);
                    }

                    using (var package = ZipFile.Open(singleBackupFile, ZipArchiveMode.Read))
                    {
                        if (package.Entries.Count == 0)
                            return;

                        var toDispose = new List<IDisposable>();

						var tempDir = Directory.CreateDirectory(Path.GetTempPath() + Guid.NewGuid()).FullName;

                        try
                        {
                            TransactionHeader* lastTxHeader = null;
                            var pagesToWrite = new Dictionary<long, Func<Page>>();

                            long journalNumber = -1;
                            foreach (var entry in package.Entries)
                            {
                                switch (Path.GetExtension(entry.Name))
                                {
                                    case ".journal":

										var jounalFileName = Path.Combine(tempDir, entry.Name);
                                        using (var output = new FileStream(jounalFileName, FileMode.Create))
                                        using (var input = entry.Open())
                                        {
                                            output.Position = output.Length;
                                            input.CopyTo(output);
                                        }

                                        var pager = new Win32MemoryMapPager(jounalFileName);
                                        toDispose.Add(pager);

                                        if (long.TryParse(Path.GetFileNameWithoutExtension(entry.Name), out journalNumber) == false)
                                        {
                                            throw new InvalidOperationException("Cannot parse journal file number");
                                        }

										var recoveryPager = new Win32MemoryMapPager(Path.Combine(tempDir, StorageEnvironmentOptions.JournalRecoveryName(journalNumber)));
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

                                        break;
                                    default:
                                        throw new InvalidOperationException("Unknown file, cannot restore: " + entry);
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

                            env.HeaderAccessor.Modify(header =>
                            {
                                header->TransactionId = lastTxHeader->TransactionId;
                                header->LastPageNumber = lastTxHeader->LastPageNumber;

                                header->Journal.LastSyncedJournal = journalNumber;
                                header->Journal.LastSyncedTransactionId = lastTxHeader->TransactionId;

                                header->Root = lastTxHeader->Root;
                                header->FreeSpace = lastTxHeader->FreeSpace;

                                header->Journal.CurrentJournal = journalNumber + 1;
                                header->Journal.JournalFilesCount = 0;
                            });
                        }
                        finally
                        {
                            toDispose.ForEach(x => x.Dispose());

	                        try
	                        {
								Directory.Delete(tempDir, true);
	                        }
	                        catch (Exception)
	                        {
								// just temp dir - ignore it
	                        }
                        }
                    }
                }
            }
        }
    }
}