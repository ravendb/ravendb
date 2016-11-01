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
using System.Security.Cryptography.X509Certificates;
using Voron.Global;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;

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

        public void ToFile(StorageEnvironment env, string backupPath,
            CompressionLevel compression = CompressionLevel.Optimal,
            Action<string> infoNotify = null,
            Action backupStarted = null)
        {
            infoNotify = infoNotify ?? (s => { });

            infoNotify("Voron backup db started");

            using (var file = new FileStream(backupPath, FileMode.Create))
            {
                using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                {
                    infoNotify("Voron backup started");
                    var dataPager = env.Options.DataPager;
                    var copier = new DataCopier(env.Options.PageSize * 16);
                    Backup(env, compression, infoNotify, backupStarted, dataPager, package, string.Empty,
                        copier);

                    file.Flush(true); // make sure that we fully flushed to disk
                }
            }

            infoNotify("Voron backup db finished");
        }
        /// <summary>
        /// Do a full backup of a set of environments. Note that the order of the environments matter!
        /// </summary>
        public void ToFile(IEnumerable<StorageEnvironmentInformation> envs, string backupPath, CompressionLevel compression = CompressionLevel.Optimal,
            Action<string> infoNotify = null,
            Action backupStarted = null)
        {
            infoNotify = infoNotify ?? (s => { });

            infoNotify("Voron backup db started");

            using (var file = new FileStream(backupPath, FileMode.Create))
            {
                using (var package = new ZipArchive(file, ZipArchiveMode.Create, leaveOpen: true))
                {
                    foreach (var e in envs)
                    {
                        infoNotify("Voron backup " + e.Name + "started");
                        var basePath = Path.Combine(e.Folder, e.Name);
                        var env = e.Env;
                        var dataPager = env.Options.DataPager;
                        var copier = new DataCopier(env.Options.PageSize * 16);
                        Backup(env, compression, infoNotify, backupStarted, dataPager, package, basePath,
                            copier);
                    }

                    file.Flush(true); // make sure that we fully flushed to disk
                }
            }

            infoNotify("Voron backup db finished");
        }

        private static void Backup(
            StorageEnvironment env, CompressionLevel compression, Action<string> infoNotify,
            Action backupStarted, AbstractPager dataPager, ZipArchive package, string basePath, DataCopier copier)
        {
            var usedJournals = new List<JournalFile>();
            long lastWrittenLogPage = -1;
            long lastWrittenLogFile = -1;
            LowLevelTransaction txr = null;
            try
            {
                long allocatedPages;
                using (var writePesistentContext = new TransactionPersistentContext(true))
                using (var readPesistentContext = new TransactionPersistentContext(true))
                using (var txw = env.NewLowLevelTransaction(writePesistentContext, TransactionFlags.ReadWrite)) // so we can snapshot the headers safely
                {
                    txr = env.NewLowLevelTransaction(readPesistentContext, TransactionFlags.Read);// now have snapshot view
                    allocatedPages = dataPager.NumberOfAllocatedPages;

                    Debug.Assert(HeaderAccessor.HeaderFileNames.Length == 2);
                    infoNotify("Voron copy headers for " + basePath);
                    VoronBackupUtil.CopyHeaders(compression, package, copier, env.Options, basePath);

                    // journal files snapshot
                    var files = env.Journal.Files; // thread safety copy

                    JournalInfo journalInfo = env.HeaderAccessor.Get(ptr => ptr->Journal);
                    for (var journalNum = journalInfo.CurrentJournal - journalInfo.JournalFilesCount + 1;
                        journalNum <= journalInfo.CurrentJournal;
                        journalNum++)
                    {
                        var journalFile = files.FirstOrDefault(x => x.Number == journalNum);
                        // first check journal files currently being in use
                        if (journalFile == null)
                        {
                            long journalSize;
                            using (var pager = env.Options.OpenJournalPager(journalNum))
                            {
                                journalSize = Bits.NextPowerOf2(pager.NumberOfAllocatedPages * env.Options.PageSize);
                            }

                            journalFile = new JournalFile(env, env.Options.CreateJournalWriter(journalNum, journalSize), journalNum);
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

                backupStarted?.Invoke();

                // data file backup
                var dataPart = package.CreateEntry(Path.Combine(basePath, Constants.DatabaseFilename), compression);
                Debug.Assert(dataPart != null);

                if (allocatedPages > 0) //only true if dataPager is still empty at backup start
                {
                    using (var dataStream = dataPart.Open())
                    {
                        // now can copy everything else
                        var firstDataPage = dataPager.Read(null, 0);

                        copier.ToStream(firstDataPage.Base, env.Options.PageSize * allocatedPages, dataStream);
                    }
                }

                try
                {
                    foreach (JournalFile journalFile in usedJournals)
                    {
                        var entryName = Path.Combine(basePath, StorageEnvironmentOptions.JournalName(journalFile.Number));
                        var journalPart = package.CreateEntry(entryName, compression);

                        Debug.Assert(journalPart != null);

                        long pagesToCopy = journalFile.JournalWriter.NumberOfAllocatedPages;
                        if (journalFile.Number == lastWrittenLogFile)
                            pagesToCopy = lastWrittenLogPage + 1;

                        using (var stream = journalPart.Open())
                        {
                            copier.ToStream(env, journalFile, 0, pagesToCopy, stream);
                            infoNotify(string.Format("Voron copy journal file {0}", entryName));
                        }
                    }
                }
                finally
                {
                    foreach (var journalFile in usedJournals)
                    {
                        journalFile.Release();
                    }
                }
            }
            finally
            {
                txr?.Dispose();
            }
        }

        public void Restore(string backupPath, string voronDataDir, string journalDir = null)
        {
            journalDir = journalDir ?? voronDataDir;

            if (Directory.Exists(voronDataDir) == false)
                Directory.CreateDirectory(voronDataDir);

            if (Directory.Exists(journalDir) == false)
                Directory.CreateDirectory(journalDir);

            using (var zip = ZipFile.Open(backupPath, ZipArchiveMode.Read, System.Text.Encoding.UTF8))
            {
                foreach (var entry in zip.Entries)
                {
                    var dst = Path.GetExtension(entry.Name) == ".journal" ? journalDir : voronDataDir;

                    var folder = Path.Combine(dst, Path.GetDirectoryName(entry.FullName));
                    if (Directory.Exists(folder) == false)
                    {
                        Directory.CreateDirectory(folder);
                    }
                    using (var input = entry.Open())
                    using (var output = new FileStream(Path.Combine(folder, entry.Name), FileMode.CreateNew))
                    {
                        input.CopyTo(output);
                    }
                }
            }
        }
    }
}