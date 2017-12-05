// -----------------------------------------------------------------------
//  <copyright file="FullBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Backup
{
    public unsafe class FullBackup
    {

        public void ToFile(StorageEnvironment env, string backupPath, CancellationToken token, CompressionLevel compression = CompressionLevel.Optimal,
            Action<string> infoNotify = null,
            Action backupStarted = null)
        {
            infoNotify = infoNotify ?? (s => { });

            var dataPager = env.Options.DataPager;
            var copier = new DataCopier(AbstractPager.PageSize * 16);
            Transaction txr = null;
            try
            {

                infoNotify("Voron copy headers");

                using (var file = new FileStream(backupPath, FileMode.Create))
                {
                    using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen:true))
                    {
                        long allocatedPages;

                        ImmutableAppendOnlyList<JournalFile> files; // thread safety copy
                        var usedJournals = new List<JournalFile>();
                        long lastWrittenLogPage = -1;
                        long lastWrittenLogFile = -1;
                        var backupSuccess = false;
                        using (var txw = env.NewTransaction(TransactionFlags.ReadWrite)) // so we can snapshot the headers safely
                        {
                            txr = env.NewTransaction(TransactionFlags.Read); // now have snapshot view
                            allocatedPages = dataPager.NumberOfAllocatedPages;

                            Debug.Assert(HeaderAccessor.HeaderFileNames.Length == 2);

                            VoronBackupUtil.CopyHeaders(compression, package, copier, env.Options);

                            // journal files snapshot
                            files = env.Journal.Files;

                            JournalInfo journalInfo = env.HeaderAccessor.Get(ptr => ptr->Journal);
                            for (var journalNum = journalInfo.CurrentJournal - journalInfo.JournalFilesCount + 1; journalNum <= journalInfo.CurrentJournal; journalNum++)
                            {
                                token.ThrowIfCancellationRequested();

                                var journalFile = files.FirstOrDefault(x => x.Number == journalNum); // first check journal files currently being in use
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
                            }

                            if (env.Journal.CurrentFile != null)
                            {
                                lastWrittenLogFile = env.Journal.CurrentFile.Number;
                                lastWrittenLogPage = env.Journal.CurrentFile.WritePagePosition - 1;
                            }

                            // txw.Commit(); intentionally not committing
                        }

                        if (backupStarted != null)
                            backupStarted();

                        // data file backup
                        var dataPart = package.CreateEntry(Constants.DatabaseFilename, compression);
                        Debug.Assert(dataPart != null);

                        if (allocatedPages > 0) //only true if dataPager is still empty at backup start
                        {
                            using (var dataStream = dataPart.Open())
                            {
                                // now can copy everything else
                                var firstDataPage = dataPager.Read(null, 0);

                                copier.ToStream(firstDataPage.Base, AbstractPager.PageSize * allocatedPages, dataStream, token);
                            }
                        }

                        try
                        {
                            long lastBackedupJournal = 0;
                            foreach (var journalFile in usedJournals)
                            {
                                var journalPart = package.CreateEntry(StorageEnvironmentOptions.JournalName(journalFile.Number), compression);

                                Debug.Assert(journalPart != null);

                                var pagesToCopy = journalFile.JournalWriter.NumberOfAllocatedPages;
                                if (journalFile.Number == lastWrittenLogFile)
                                    pagesToCopy = lastWrittenLogPage + 1;

                                using (var stream = journalPart.Open())
                                {
                                    copier.ToStream(journalFile, 0, pagesToCopy, stream, token);
                                    infoNotify(string.Format("Voron copy journal file {0} ", journalFile));
                                }
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
                    file.Flush(true); // make sure that we fully flushed to disk
                }
            }
            finally
            {
                if (txr != null)
                    txr.Dispose();
            }
            infoNotify(string.Format("Voron backup db finished"));
        }

        public void Restore(string backupPath, string voronDataDir, string journalDir = null)
        {
            journalDir = journalDir ?? voronDataDir;

            if (Directory.Exists(voronDataDir) == false)
                Directory.CreateDirectory(voronDataDir);

            if (Directory.Exists(journalDir) == false)
                Directory.CreateDirectory(journalDir);

            using (var zip = ZipFile.Open(backupPath,ZipArchiveMode.Read, System.Text.Encoding.UTF8))
            {
                foreach (var entry in zip.Entries)
                {
                    var dst = Path.GetExtension(entry.Name) == ".journal" ? journalDir : voronDataDir;
                    using (var input = entry.Open())
                    using (var output = new FileStream(Path.Combine(dst, entry.Name), FileMode.CreateNew))
                    {
                        input.CopyTo(output);
                    }
                }
            }
        }
    }
}
