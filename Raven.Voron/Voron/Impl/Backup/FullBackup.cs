// -----------------------------------------------------------------------
//  <copyright file="FullBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Threading;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Backup
{
	public unsafe class FullBackup
	{
		public void ToFile(StorageEnvironment env, string backupPath, CompressionLevel compression = CompressionLevel.Optimal)
		{			
			var dataPager = env.Options.DataPager;
			var copier = new DataCopier(AbstractPager.PageSize * 16);
			Transaction txr = null;
			try
			{
				using (var file = new FileStream(backupPath, FileMode.Create))
				using (var package = new ZipArchive(file, ZipArchiveMode.Create))
				{
					long allocatedPages;

					ImmutableAppendOnlyList<JournalFile> files; // thread safety copy
					long lastWrittenLogPage = -1;
					long lastWrittenLogFile = -1;
					using (var txw = env.NewTransaction(TransactionFlags.ReadWrite)) // so we can snapshot the headers safely
					{
						txr = env.NewTransaction(TransactionFlags.Read); // now have snapshot view
						allocatedPages = dataPager.NumberOfAllocatedPages;

						Debug.Assert(HeaderAccessor.HeaderFileNames.Length == 2);

                        VoronBackupUtil.CopyHeaders(compression, package, copier, env.Options);

						// journal files snapshot
						files = env.Journal.Files;

						foreach (var journalFile in files)
						{
							journalFile.AddRef();
						}

						if (env.Journal.CurrentFile != null)
						{
							lastWrittenLogFile = env.Journal.CurrentFile.Number;
							lastWrittenLogPage = env.Journal.CurrentFile.WritePagePosition - 1;
						}

						// txw.Commit(); intentionally not committing
					}

					// data file backup
					var dataPart = package.CreateEntry(Constants.DatabaseFilename, compression);
					Debug.Assert(dataPart != null);

					if (allocatedPages > 0) //only true if dataPager is still empty at backup start
					{
						using (var dataStream = dataPart.Open())
						{
							// now can copy everything else
							var firstDataPage = dataPager.Read(0);

							copier.ToStream(firstDataPage.Base, AbstractPager.PageSize*allocatedPages, dataStream);
						}
					}

					try
					{
						foreach (var journalFile in files)
						{
							var journalPart = package.CreateEntry(StorageEnvironmentOptions.JournalName(journalFile.Number), compression);

							Debug.Assert(journalPart != null);

							var pagesToCopy = journalFile.JournalWriter.NumberOfAllocatedPages;
							if (journalFile.Number == lastWrittenLogFile)
								pagesToCopy = lastWrittenLogPage + 1;

							using (var stream = journalPart.Open())
							{
								copier.ToStream(journalFile, 0, pagesToCopy, stream);
							}
						}
					}
					finally
					{
						foreach (var journalFile in files)
						{
							journalFile.Release();
						}
					}
				}
			}
			finally
			{
				if (txr != null)
					txr.Dispose();
			}
		}

	    public void Restore(string backupPath, string voronDataDir, string journalDir = null)
		{
		    journalDir = journalDir ?? voronDataDir;

			if (Directory.Exists(voronDataDir) == false)
				Directory.CreateDirectory(voronDataDir);

			if (Directory.Exists(journalDir) == false)
				Directory.CreateDirectory(journalDir);

		    using (var zip = ZipFile.OpenRead(backupPath))
		    {
		        foreach (var entry in zip.Entries)
		        {
		            var dst = Path.GetExtension(entry.Name) == ".journal" ? journalDir : voronDataDir;
		            using (var input = entry.Open())
                    using(var output = new FileStream(Path.Combine(dst, entry.Name), FileMode.CreateNew))
		            {
		                input.CopyTo(output);
		            }
		        }
		    }
		}
	}
}