// -----------------------------------------------------------------------
//  <copyright file="FullBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Diagnostics;
using System.IO;
using System.IO.Compression;
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

						foreach (var headerFileName in HeaderAccessor.HeaderFileNames)
						{
							var header = stackalloc FileHeader[1];

							if (env.Options.ReadHeader(headerFileName, header))
							{
								var headerPart = package.CreateEntry(headerFileName, compression);
								Debug.Assert(headerPart != null);

								using (var headerStream = headerPart.Open())
								{
									copier.ToStream((byte*) header, sizeof (FileHeader), headerStream);
								}
							}
						}

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
					var dataPart = package.CreateEntry("db.voron", compression);
					Debug.Assert(dataPart != null);

					using (var dataStream = dataPart.Open())
					{
						// now can copy everything else
						var firstDataPage = dataPager.Read(0);

						copier.ToStream(firstDataPage.Base, AbstractPager.PageSize*allocatedPages, dataStream);
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

		public void Restore(string backupPath, string voronDataDir)
		{
			ZipFile.ExtractToDirectory(backupPath, voronDataDir);
		}
	}
}