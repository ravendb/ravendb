// -----------------------------------------------------------------------
//  <copyright file="FullBackup.cs" company="Hibernating Rhinos LTD">
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
using System.Threading;
using Sparrow.Utils;
using Voron.Global;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;
using Voron.Util.Settings;

namespace Voron.Impl.Backup
{
    public unsafe class FullBackup
    {
        public class StorageEnvironmentInformation
        {
            public string Folder { set; get; }
            public string Name { get; set; }
            public StorageEnvironment Env { get; set; }
        }

        public void ToFile(StorageEnvironment env, VoronPathSetting backupPath,
            CompressionLevel compression = CompressionLevel.Optimal,
            Action<(string Message, int FilesCount)> infoNotify = null)
        {
            infoNotify = infoNotify ?? (_ => { });

            infoNotify(("Voron backup db started", 0));

            using (var file = SafeFileStream.Create(backupPath.FullPath, FileMode.Create))
            {
                using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                {
                    infoNotify(("Voron backup started", 0));
                    var dataPager = env.Options.DataPager;
                    var copier = new DataCopier(Constants.Storage.PageSize * 16);
                    Backup(env, compression, dataPager, package, string.Empty, copier, infoNotify);

                    file.Flush(true); // make sure that we fully flushed to disk
                }
            }

            infoNotify(("Voron backup db finished", 0));
        }
        /// <summary>
        /// Do a full backup of a set of environments. Note that the order of the environments matter!
        /// </summary>
        public void ToFile(IEnumerable<StorageEnvironmentInformation> envs,
            ZipArchive archive,
            CompressionLevel compression = CompressionLevel.Optimal,
            Action<(string Message, int FilesCount)> infoNotify = null,
            CancellationToken cancellationToken = default)
        {
            infoNotify = infoNotify ?? (_ => { });
            infoNotify(("Voron backup db started", 0));

            foreach (var e in envs)
            {
                infoNotify(($"Voron backup {e.Name} started", 0));
                var basePath = Path.Combine(e.Folder, e.Name);

                var env = e.Env;
                var dataPager = env.Options.DataPager;
                var copier = new DataCopier(Constants.Storage.PageSize * 16);
                Backup(env, compression, dataPager, archive, basePath, copier, infoNotify, cancellationToken);
            }

            infoNotify(("Voron backup db finished", 0));
        }

        private static void Backup(
            StorageEnvironment env, CompressionLevel compression, AbstractPager dataPager,
            ZipArchive package, string basePath, DataCopier copier,
            Action<(string Message, int FilesCount)> infoNotify,
            CancellationToken cancellationToken = default)
        {
            var usedJournals = new List<JournalFile>();
            long lastWrittenLogPage = -1;
            long lastWrittenLogFile = -1;
            LowLevelTransaction txr = null;
            var backupSuccess = false;

            try
            {
                long allocatedPages;
                var writePesistentContext = new TransactionPersistentContext(true);
                var readPesistentContext = new TransactionPersistentContext(true);
                using (var txw = env.NewLowLevelTransaction(writePesistentContext, TransactionFlags.ReadWrite)) // so we can snapshot the headers safely
                {
                    txr = env.NewLowLevelTransaction(readPesistentContext, TransactionFlags.Read);// now have snapshot view
                    allocatedPages = dataPager.NumberOfAllocatedPages;

                    Debug.Assert(HeaderAccessor.HeaderFileNames.Length == 2);
                    infoNotify(($"Voron copy headers for {basePath}", 2));
                    VoronBackupUtil.CopyHeaders(compression, package, copier, env.Options, basePath);

                    // journal files snapshot
                    var files = env.Journal.Files; // thread safety copy

                    JournalInfo journalInfo = env.HeaderAccessor.Get(ptr => ptr->Journal);
                    var startingJournal = journalInfo.LastSyncedJournal;
                    if (env.Options.JournalExists(startingJournal) == false && 
                        journalInfo.Flags.HasFlag(JournalInfoFlags.IgnoreMissingLastSyncJournal) || 
                        startingJournal == -1)
                    {
                        startingJournal++;
                    }

                    for (var journalNum = startingJournal;
                        journalNum <= journalInfo.CurrentJournal;
                        journalNum++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var journalFile = files.FirstOrDefault(x => x.Number == journalNum);
                        // first check journal files currently being in use
                        if (journalFile == null)
                        {
                            long journalSize;
                            using (var pager = env.Options.OpenJournalPager(journalNum))
                            {
                                journalSize = Bits.NextPowerOf2(pager.NumberOfAllocatedPages * Constants.Storage.PageSize);
                            }

                            journalFile = new JournalFile(env, env.Options.CreateJournalWriter(journalNum, journalSize), journalNum);
                        }

                        journalFile.AddRef();
                        usedJournals.Add(journalFile);
                    }

                    if (env.Journal.CurrentFile != null)
                    {
                        lastWrittenLogFile = env.Journal.CurrentFile.Number;
                        lastWrittenLogPage = env.Journal.CurrentFile.WritePosIn4KbPosition - 1;
                    }

                    // txw.Commit(); intentionally not committing
                }

                // data file backup
                var dataPart = package.CreateEntry(Path.Combine(basePath, Constants.DatabaseFilename), compression);
                Debug.Assert(dataPart != null);

                if (allocatedPages > 0) //only true if dataPager is still empty at backup start
                {
                    using (var dataStream = dataPart.Open())
                    {
                        // now can copy everything else
                        copier.ToStream(dataPager, 0, allocatedPages, dataStream, message => infoNotify((message, 0)), cancellationToken);
                    }
                    infoNotify(("Voron copy data file", 1));
                }

                try
                {
                    long lastBackedupJournal = 0;
                    foreach (var journalFile in usedJournals)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        var entryName = StorageEnvironmentOptions.JournalName(journalFile.Number);
                        var journalPart = package.CreateEntry(Path.Combine(basePath, entryName), compression);

                        Debug.Assert(journalPart != null);

                        long pagesToCopy = journalFile.JournalWriter.NumberOfAllocated4Kb;
                        if (journalFile.Number == lastWrittenLogFile)
                            pagesToCopy = lastWrittenLogPage + 1;

                        using (var stream = journalPart.Open())
                        {
                            copier.ToStream(env, journalFile, 0, pagesToCopy, stream, message => infoNotify((message, 0)), cancellationToken);
                        }
                        infoNotify(($"Voron copy journal file {entryName}", 1));

                        lastBackedupJournal = journalFile.Number;
                    }

                    if (env.Options.IncrementalBackupEnabled)
                    {
                        env.HeaderAccessor.Modify(header =>
                        {
                            header->IncrementalBackup.LastBackedUpJournal = lastBackedupJournal;

                            //since we backed-up everything, no need to start next incremental backup from the middle
                            header->IncrementalBackup.LastBackedUpJournalPage = -1;
                        });
                    }
                    backupSuccess = true;
                }
                catch (Exception)
                {
                    backupSuccess = false;
                    throw;
                }
                finally
                {
                    var lastSyncedJournal = env.HeaderAccessor.Get(header => header->Journal).LastSyncedJournal;
                    foreach (var journalFile in usedJournals)
                    {
                        if (backupSuccess) // if backup succeeded we can remove journals
                        {
                            if (journalFile.Number < lastWrittenLogFile &&  // prevent deletion of the current journal and journals with a greater number
                                journalFile.Number < lastSyncedJournal) // prevent deletion of journals that aren't synced with the data file
                            {
                                journalFile.DeleteOnClose = true;
                            }
                        }

                        journalFile.Release();
                    }
                }
            }
            finally
            {
                txr?.Dispose();
            }
        }

        public void Restore(VoronPathSetting backupPath,
            VoronPathSetting voronDataDir,
            VoronPathSetting journalDir = null,
            Action<string> onProgress = null,
            CancellationToken cancellationToken = default)
        {
            using (var zip = ZipFile.Open(backupPath.FullPath, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
                Restore(zip.Entries, voronDataDir, journalDir, onProgress, cancellationToken);
        }

        public void Restore(IEnumerable<ZipArchiveEntry> entries,
            VoronPathSetting voronDataDir,
            VoronPathSetting journalDir = null,
            Action<string> onProgress = null,
            CancellationToken cancellationToken = default)
        {
            journalDir = journalDir ?? voronDataDir.Combine("Journals");

            if (Directory.Exists(voronDataDir.FullPath) == false)
                Directory.CreateDirectory(voronDataDir.FullPath);

            if (Directory.Exists(journalDir.FullPath) == false)
                Directory.CreateDirectory(journalDir.FullPath);

            onProgress?.Invoke("Starting snapshot restore");

            foreach (var entry in entries)
            {
                var dst = string.Equals(Path.GetExtension(entry.Name), ".journal", StringComparison.OrdinalIgnoreCase)
                    ? journalDir
                    : voronDataDir;

                var sw = Stopwatch.StartNew();

                if (Directory.Exists(dst.FullPath) == false)
                    Directory.CreateDirectory(dst.FullPath);

                using (var input = entry.Open())
                using (var output = SafeFileStream.Create(dst.Combine(entry.Name).FullPath, FileMode.CreateNew))
                {
                    input.CopyTo(output, cancellationToken);
                }

                onProgress?.Invoke($"Restored file: '{entry.Name}' to: '{dst}', " +
                                   $"size in bytes: {entry.Length:#,#;;0}, " +
                                   $"took: {sw.ElapsedMilliseconds:#,#;;0}ms");
            }
        }
    }
}
