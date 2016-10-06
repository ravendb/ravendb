// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using Voron.Data.BTrees;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Global;
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

        public long ToFile(StorageEnvironment env, string backupPath, CompressionLevel compression = CompressionLevel.Optimal,
            Action<string> infoNotify = null,
            Action backupStarted = null)
        {
            infoNotify = infoNotify ?? (s => { });

            if (env.Options.IncrementalBackupEnabled == false)
                throw new InvalidOperationException("Incremental backup is disabled for this storage");

            var copier = new DataCopier(env.Options.PageSize * 16);

            using (var file = new FileStream(backupPath, FileMode.Create))
            {
                long numberOfBackedUpPages;
                using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                {
                    numberOfBackedUpPages = Incremental_Backup(env, compression, infoNotify, 
                        backupStarted,  package, string.Empty, copier);
                }
                file.Flush(true); // make sure that this is actually persisted fully to disk
                return numberOfBackedUpPages;
            }
        }

        /// <summary>
        /// Do a incremental backup of a set of environments. Note that the order of the environments matter!
        /// </summary>
        public long ToFile(IEnumerable<FullBackup.StorageEnvironmentInformation> envs, string backupPath, CompressionLevel compression = CompressionLevel.Optimal,
            Action<string> infoNotify = null,
            Action backupStarted = null)
        {
            infoNotify = infoNotify ?? (s => { });

            long totalNumberOfBackedUpPages = 0;
            using (var file = new FileStream(backupPath, FileMode.Create))
            {
                using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                {
                    foreach (var e in envs)
                    {
                        if (e.Env.Options.IncrementalBackupEnabled == false)
                            throw new InvalidOperationException("Incremental backup is disabled for this storage");
                        infoNotify("Voron backup " + e.Name + "started");
                        var basePath = Path.Combine(e.Folder, e.Name);
                        var env = e.Env;
                        var copier = new DataCopier(env.Options.PageSize * 16);
                        var numberOfBackedUpPages = Incremental_Backup(env, compression, infoNotify,
                                                backupStarted,  package, basePath, copier);
                        totalNumberOfBackedUpPages += numberOfBackedUpPages;
                    }
                }
                file.Flush(true); // make sure that this is actually persisted fully to disk

                return totalNumberOfBackedUpPages;
            }
        }

        private static long Incremental_Backup(StorageEnvironment env, CompressionLevel compression, Action<string> infoNotify,
            Action backupStarted, ZipArchive package, string basePath, DataCopier copier)
        {
            long numberOfBackedUpPages = 0;
            long lastWrittenLogFile = -1;
            long lastWrittenLogPage = -1;
            bool backupSuccess = true;
            IncrementalBackupInfo backupInfo;
            using (var txw = env.NewLowLevelTransaction(TransactionFlags.ReadWrite))
            {
                backupInfo = env.HeaderAccessor.Get(ptr => ptr->IncrementalBackup);

                if (env.Journal.CurrentFile != null)
                {
                    lastWrittenLogFile = env.Journal.CurrentFile.Number;
                    lastWrittenLogPage = env.Journal.CurrentFile.WritePagePosition;
                }

                // txw.Commit(); intentionally not committing
            }

            using (env.NewLowLevelTransaction(TransactionFlags.Read))
            {
                backupStarted?.Invoke(); // we let call know that we have started the backup 

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
                        var num = journalNum;

                        var journalFile = GetJournalFile(env, journalNum, backupInfo);

                        journalFile.AddRef();

                        usedJournals.Add(journalFile);

                        var startBackupAt = 0L;
                        long pagesToCopy = journalFile.JournalWriter.NumberOfAllocatedPages;
                        if (journalFile.Number == backupInfo.LastBackedUpJournal)
                        {
                            startBackupAt = backupInfo.LastBackedUpJournalPage + 1;
                            pagesToCopy -= startBackupAt;
                        }

                        if (startBackupAt >= journalFile.JournalWriter.NumberOfAllocatedPages) // nothing to do here
                            continue;

                        var part = package.CreateEntry(Path.Combine(basePath, StorageEnvironmentOptions.JournalName(journalNum))
                                                        , compression);
                        Debug.Assert(part != null);

                        if (journalFile.Number == lastWrittenLogFile)
                            pagesToCopy -= (journalFile.JournalWriter.NumberOfAllocatedPages - lastWrittenLogPage);

                        using (var stream = part.Open())
                        {
                            copier.ToStream(env, journalFile, startBackupAt, pagesToCopy, stream);
                            infoNotify(string.Format("Voron Incr copy journal number {0}", num));
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
                    var lastSyncedJournal = env.HeaderAccessor.Get(header => header->Journal).LastSyncedJournal;

                    foreach (var jrnl in usedJournals)
                    {
                        if (backupSuccess) // if backup succeeded we can remove journals
                        {
                            if (jrnl.Number < lastWrittenLogFile &&
                                // prevent deletion of the current journal and journals with a greater number
                                jrnl.Number < lastSyncedJournal)
                            // prevent deletion of journals that aren't synced with the data file
                            {
                                jrnl.DeleteOnClose = true;
                            }
                        }

                        jrnl.Release();
                    }
                }
                infoNotify(string.Format("Voron Incr Backup total {0} pages", numberOfBackedUpPages));
            }
            return numberOfBackedUpPages;
        }

        internal static JournalFile GetJournalFile(StorageEnvironment env, long journalNum, IncrementalBackupInfo backupInfo)
        {
            var journalFile = env.Journal.Files.FirstOrDefault(x => x.Number == journalNum); // first check journal files currently being in use
            if (journalFile != null)
            {
                journalFile.AddRef();
                return journalFile;
            }
            try
            {
                using (var pager = env.Options.OpenJournalPager(journalNum))
                {
                    long journalSize = Bits.NextPowerOf2(pager.NumberOfAllocatedPages * env.Options.PageSize);
                    journalFile = new JournalFile(env.Options.CreateJournalWriter(journalNum, journalSize), journalNum);
                    journalFile.AddRef();
                    return journalFile;
                }
            }
            catch (Exception e)
            {
                if (backupInfo.LastBackedUpJournal == -1 && journalNum == 0 && e.Message.StartsWith("No such journal", StringComparison.Ordinal))
                {
                    throw new InvalidOperationException("The first incremental backup creation failed because the first journal file " +
                                                        StorageEnvironmentOptions.JournalName(journalNum) + " was not found. " +
                                                        "Did you turn on the incremental backup feature after initializing the storage? " +
                                                        "In order to create backups incrementally the storage must be created with IncrementalBackupEnabled option set to 'true'.", e);
                }

                throw;
            }

        }

        public void Restore(StorageEnvironmentOptions options, IEnumerable<string> backupPaths)
        {
            var ownsPagers = options.OwnsPagers;
            options.OwnsPagers = false;
            try
            {
                using (var env = new StorageEnvironment(options))
                {
                    foreach (var backupPath in backupPaths)
                    {
                        Restore(env, backupPath);
                    }
                }
            }
            finally
            {
                options.OwnsPagers = ownsPagers;
            }
        }

        public void Restore(string outPath, IEnumerable<string> backupPaths)
        {
            foreach (var backupPath in backupPaths)
            {
                using (var package = ZipFile.Open(backupPath, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
                {
                    if (package.Entries.Count == 0)
                        return;
                    foreach (var dir in package.Entries.GroupBy(entry => Path.GetDirectoryName(entry.FullName)))
                    {
                        using (var options = StorageEnvironmentOptions.ForPath(Path.Combine(outPath, dir.Key)))
                        {
                            using (var env = new StorageEnvironment(options))
                            {
                                Restore(env, dir);
                            }
                        }
                    }
                }
            }
        }

        private void Restore(StorageEnvironment env, string singleBackupFile)
        {
            using (env.Journal.Applicator.TakeFlushingLock())
            {
                using (var txw = env.NewLowLevelTransaction(TransactionFlags.ReadWrite))
                {
                    using (env.Options.AllowManualFlushing())
                    {
                        env.FlushLogToDataFile(txw);
                    }

                    using (var package = ZipFile.Open(singleBackupFile, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
                    {
                        if (package.Entries.Count == 0)
                            return;

                        var toDispose = new List<IDisposable>();

                        var tempDir = Directory.CreateDirectory(Path.GetTempPath() + Guid.NewGuid()).FullName;

                        Restore(env, package.Entries, tempDir, toDispose, txw);
                    }
                }
            }
        }

        private void Restore(StorageEnvironment env, IEnumerable<ZipArchiveEntry> entries)
        {
            using (env.Journal.Applicator.TakeFlushingLock())
            {
                using (var txw = env.NewLowLevelTransaction(TransactionFlags.ReadWrite))
                {
                    using (env.Options.AllowManualFlushing())
                    {
                        env.FlushLogToDataFile(txw);
                    }

                    var toDispose = new List<IDisposable>();

                    var tempDir = Directory.CreateDirectory(Path.GetTempPath() + Guid.NewGuid()).FullName;

                    Restore(env, entries, tempDir, toDispose, txw);
                }
            }
        }
        private static void Restore(StorageEnvironment env, IEnumerable<ZipArchiveEntry> entries, string tempDir, List<IDisposable> toDispose,
            LowLevelTransaction txw)
        {
            try
            {
                TransactionHeader* lastTxHeader = null;

                long journalNumber = -1;
                foreach (var entry in entries)
                {
                    switch (Path.GetExtension(entry.Name))
                    {
                        case ".merged-journal":
                        case ".journal":

                            var jounalFileName = Path.Combine(tempDir, entry.Name);
                            using (var output = new FileStream(jounalFileName, FileMode.Create))
                            using (var input = entry.Open())
                            {
                                output.Position = output.Length;
                                input.CopyTo(output);
                            }

                            var pager = env.Options.OpenPager(jounalFileName);
                            toDispose.Add(pager);

                            if (long.TryParse(Path.GetFileNameWithoutExtension(entry.Name), out journalNumber) == false)
                            {
                                throw new InvalidOperationException("Cannot parse journal file number");
                            }

                            var recoveryPager =
                                env.Options.CreateScratchPager(Path.Combine(tempDir,
                                    StorageEnvironmentOptions.JournalRecoveryName(journalNumber)),
                                    env.Options.InitialFileSize ?? env.Options.InitialLogFileSize);
                            toDispose.Add(recoveryPager);

                            var reader = new JournalReader(pager, env.Options.DataPager, recoveryPager, 0, lastTxHeader);

                            while (reader.ReadOneTransactionToDataFile(env.Options))
                            {
                                lastTxHeader = reader.LastTransactionHeader;
                            }
                            break;

                        default:
                            throw new InvalidOperationException("Unknown file, cannot restore: " + entry);
                    }
                }

                if (lastTxHeader == null)
                    return; // there was no valid transactions, nothing to do

                env.Options.DataPager.Sync();


                var root = Tree.Open(txw, null, &lastTxHeader->Root);
                root.Name = Constants.RootTreeNameSlice;

                txw.UpdateRootsIfNeeded(root);

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
                catch
                {
                    // this is just a temporary directory, the worst case scenario is that we dont reclaim the space from the OS temp directory 
                    // if for some reason we cannot delete it we are safe to ignore it.
                }
            }
        }
    }
}