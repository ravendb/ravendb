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
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron.Data.BTrees;
using Voron.Exceptions;
using Voron.Impl.Journal;
using Voron.Global;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util;
using Voron.Util.Settings;

namespace Voron.Impl.Backup
{
    public sealed unsafe class IncrementalBackup
    {
        public sealed class IncrementalRestorePaths
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

            var copier = new DataCopier(Constants.Storage.PageSize * 16);

            using (var file = SafeFileStream.Create(backupPath, FileMode.Create))
            {
                long numberOfBackedUpPages;
                using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                {
                    numberOfBackedUpPages = Incremental_Backup(env, compression, infoNotify,
                        backupStarted, package, string.Empty, copier);
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
            using (var file = SafeFileStream.Create(backupPath, FileMode.Create))
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
                        var copier = new DataCopier(Constants.Storage.PageSize * 16);
                        var numberOfBackedUpPages = Incremental_Backup(env, compression, infoNotify,
                                                backupStarted, package, basePath, copier);
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
            long lastWrittenLog4kb = -1;
            bool backupSuccess = true;
            IncrementalBackupInfo backupInfo;
            JournalInfo journalInfo;

            var transactionPersistentContext = new TransactionPersistentContext(true);
            using (var txw = env.NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite))
            {
                backupInfo = env.HeaderAccessor.Get(ptr => ptr->IncrementalBackup);
                journalInfo = env.HeaderAccessor.Get(ptr => ptr->Journal);

                if (env.Journal.CurrentFile != null)
                {
                    lastWrittenLogFile = env.Journal.CurrentFile.Number;
                    lastWrittenLog4kb = env.Journal.CurrentFile.GetWritePosIn4KbPosition(env.CurrentStateRecord);
                }

                // txw.Commit(); intentionally not committing
            }


            using (env.NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.Read))
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

                    for (var journalNum = firstJournalToBackup;
                        journalNum <= backupInfo.LastCreatedJournal;
                        journalNum++)
                    {
                        var num = journalNum;

                        var journalFile = GetJournalFile(env, journalNum, backupInfo, journalInfo);

                        journalFile.AddRef();

                        usedJournals.Add(journalFile);

                        var startBackupAt = 0L;
                        long numberOf4KbsToCopy = journalFile.JournalWriter.NumberOfAllocated4Kb;
                        if (journalFile.Number == backupInfo.LastBackedUpJournal)
                        {
                            startBackupAt = backupInfo.LastBackedUpJournalPage + 1;
                            numberOf4KbsToCopy -= startBackupAt;
                        }

                        if (startBackupAt >= journalFile.JournalWriter.NumberOfAllocated4Kb) // nothing to do here
                            continue;

                        var part =
                            package.CreateEntry(
                                Path.Combine(basePath, StorageEnvironmentOptions.JournalName(journalNum))
                                , compression);
                        Debug.Assert(part != null);

                        if (journalFile.Number == lastWrittenLogFile)
                            numberOf4KbsToCopy -= (journalFile.JournalWriter.NumberOfAllocated4Kb - lastWrittenLog4kb);

                        using (var stream = part.Open())
                        {
                            copier.ToStream(env, journalFile, startBackupAt, numberOf4KbsToCopy, stream);
                            infoNotify(string.Format("Voron Incr copy journal number {0}", num));
                        }

                        lastBackedUpFile = journalFile.Number;
                        if (journalFile.Number == backupInfo.LastCreatedJournal)
                        {
                            lastBackedUpPage = startBackupAt + numberOf4KbsToCopy - 1;
                            // we used all of this file, so the next backup should start in the next file
                            if (lastBackedUpPage == (journalFile.JournalWriter.NumberOfAllocated4Kb - 1))
                            {
                                lastBackedUpPage = -1;
                                lastBackedUpFile++;
                            }
                        }

                        numberOfBackedUpPages += numberOf4KbsToCopy;
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

        internal static JournalFile GetJournalFile(StorageEnvironment env, long journalNum, IncrementalBackupInfo backupInfo, JournalInfo journalInfo)
        {
            var journalFile = env.Journal.Files.FirstOrDefault(x => x.Number == journalNum); // first check journal files currently being in use
            if (journalFile != null)
            {
                journalFile.AddRef();
                return journalFile;
            }
            try
            {
                long journalSize = Bits.PowerOf2(env.Options.GetJournalFileSize(journalNum, journalInfo));
                journalFile = new JournalFile(env, env.Options.CreateJournalWriter(journalNum, journalSize), journalNum);
                journalFile.AddRef();
                return journalFile;
            }
            catch (InvalidJournalException e)
            {
                if (backupInfo.LastBackedUpJournal == -1 && journalNum == 0)
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
                options.ManualFlushing = true;
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

        private void Restore(StorageEnvironment env, string singleBackupFile)
        {
            using (env.Journal.Applicator.TakeFlushingLock())
            {
                env.FlushLogToDataFile();

                var transactionPersistentContext = new TransactionPersistentContext(true);
                using (var txw = env.NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite))
                {
                    using (var package = ZipFile.Open(singleBackupFile, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
                    {
                        if (package.Entries.Count == 0)
                            return;

                        var toDispose = new List<IDisposable>();

                        var tempDir = Directory.CreateDirectory(Path.GetTempPath() + Guid.NewGuid()).FullName;
                        var tempDirSettings = new VoronPathSetting(tempDir);

                        Restore(env, package.Entries, tempDirSettings, toDispose, txw);
                    }
                }
            }
        }

        private void Restore(StorageEnvironment env, IEnumerable<ZipArchiveEntry> entries)
        {
            using (env.Journal.Applicator.TakeFlushingLock())
            {
                env.FlushLogToDataFile();

                var transactionPersistentContext = new TransactionPersistentContext(true);
                using (var txw = env.NewLowLevelTransaction(transactionPersistentContext, TransactionFlags.ReadWrite))
                {

                    var toDispose = new List<IDisposable>();

                    var tempDir = Directory.CreateDirectory(Path.GetTempPath() + Guid.NewGuid()).FullName;
                    var tempDirSettings = new VoronPathSetting(tempDir);

                    Restore(env, entries, tempDirSettings, toDispose, txw);
                }
            }
        }

        private static void Restore(StorageEnvironment env, IEnumerable<ZipArchiveEntry> entries, VoronPathSetting tempDir, List<IDisposable> toDispose,
            LowLevelTransaction txw)
        {
            try
            {
                TransactionHeader* lastTxHeader = null;
                var lastTxHeaderStackLocation = stackalloc TransactionHeader[1];
                long lastTxId = env.HeaderAccessor.Get(x => x->TransactionId);

                long journalNumber = -1;
                var rc = Pal.rvn_pager_get_file_handle(txw.DataPagerState.Handle, out var fileHandle, out int errorCode);
                if(rc != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(rc, errorCode, "Failed to get a file handle to " + txw.DataPager.FileName);
                using var _ = fileHandle;
                foreach (var entry in entries)
                {
                    switch (Path.GetExtension(entry.Name))
                    {
                        case ".merged-journal":
                        case ".journal":

                            var jounalFileName = tempDir.Combine(entry.Name);
                            using (var output = SafeFileStream.Create(jounalFileName.FullPath, FileMode.Create))
                            using (var input = entry.Open())
                            {
                                output.Position = output.Length;
                                input.CopyTo(output);
                            }

                            var (journalPager, journalPagerState) = env.Options.OpenJournalPager(jounalFileName.FullPath);
                            toDispose.Add(journalPager);

                            if (long.TryParse(Path.GetFileNameWithoutExtension(entry.Name), out journalNumber) == false)
                            {
                                throw new InvalidOperationException("Cannot parse journal file number");
                            }

                            var (recoveryPager, recoverPagerState) =
                                env.Options.CreateTemporaryBufferPager(
                                    Path.Combine(tempDir.Combine(StorageEnvironmentOptions.JournalRecoveryName(journalNumber)).FullPath),
                                    env.Options.InitialFileSize ?? env.Options.InitialLogFileSize,
                                    env.Options.Encryption.IsEnabled);
                            toDispose.Add(recoveryPager);
                            
                            using (var reader = new JournalReader(env, journalPager, journalPagerState, txw.DataPager, recoveryPager, new HashSet<long>(),
                                       new JournalInfo { LastSyncedTransactionId = lastTxId }, new FileHeader { HeaderRevision = -1 }, lastTxHeader))
                            {
                                while (reader.ReadOneTransactionToDataFile(ref txw.DataPagerState, ref recoverPagerState, ref txw.PagerTransactionState,fileHandle, env.Options))
                                {
                                    lastTxHeader = reader.LastTransactionHeader;
                                }

                                reader.ZeroRecoveryBufferIfNeeded(recoverPagerState, ref txw.PagerTransactionState, env.Options);
                                if (lastTxHeader != null)
                                {
                                    *lastTxHeaderStackLocation = *lastTxHeader;
                                    lastTxHeader = lastTxHeaderStackLocation;
                                    lastTxId = lastTxHeader->TransactionId;
                                }
                            }

                            break;

                        default:
                            throw new InvalidOperationException("Unknown file, cannot restore: " + entry);
                    }
                }

                if (lastTxHeader == null)
                    return; // there was no valid transactions, nothing to do
                
                txw.DataPager.Sync(txw.DataPagerState, 0);
                
                var root = Tree.Open(txw, null, Constants.RootTreeNameSlice, lastTxHeader->Root);
                
                txw.UpdateRootsIfNeeded(root);

                txw.SetNextPageNumber(lastTxHeader->LastPageNumber + 1);
                
                txw.Commit();
                
                env.HeaderAccessor.Modify(header =>
                {
                    header->TransactionId = lastTxHeader->TransactionId;
                    header->LastPageNumber = lastTxHeader->LastPageNumber;
                    
                    header->Journal.LastSyncedTransactionId = lastTxHeader->TransactionId;
                
                    header->Root = lastTxHeader->Root;
                    
                    Sparrow.Memory.Set(header->Journal.Reserved, 0, JournalInfo.NumberOfReservedBytes);
                    header->Journal.Flags = JournalInfoFlags.None;
                });
            }
            finally
            {
                toDispose.ForEach(x => x.Dispose());

                try
                {
                    Directory.Delete(tempDir.FullPath, true);
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
