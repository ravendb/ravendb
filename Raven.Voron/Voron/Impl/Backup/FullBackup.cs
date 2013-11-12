// -----------------------------------------------------------------------
//  <copyright file="FullBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.IO.Packaging;
using Voron.Impl.Journal;
using Voron.Util;

namespace Voron.Impl.Backup
{
	public unsafe class FullBackup
	{
		public void ToFile(StorageEnvironment env, string backupPath, CompressionOption compression = CompressionOption.Normal)
		{
			var dataPager = env.Options.DataPager;
			var copier = new DataCopier(dataPager.PageSize*16);
			Transaction txr = null;
			try
			{
				using (var package = Package.Open(backupPath, FileMode.Create))
				{
					// data file backup
					var dataPart = package.CreatePart(new Uri("/db.voron", UriKind.Relative),
													  System.Net.Mime.MediaTypeNames.Application.Octet,
													  compression);
					Debug.Assert(dataPart != null);

					var dataStream = dataPart.GetStream();

					long allocatedPages;

					ImmutableList<JournalFile> files; // thread safety copy
					long lastWrittenLogPage = -1;
					long lastWrittenLogFile = -1;
					using (var txw = env.NewTransaction(TransactionFlags.ReadWrite)) // so we can snapshot the headers safely
					{
						txr = env.NewTransaction(TransactionFlags.Read); // now have snapshot view
						allocatedPages = dataPager.NumberOfAllocatedPages;

						// log files backup

						files = env.Journal.Files;

						files.ForEach(x => x.AddRef());

						txw.Rollback(); // will move back the current journal write position moved by current tx (this tx won't be committed anyways)

						if (env.Journal.CurrentFile != null)
						{
							lastWrittenLogFile = env.Journal.CurrentFile.Number;
							lastWrittenLogPage = env.Journal.CurrentFile.WritePagePosition - 1;
						}

						var firstPage = dataPager.Read(0);

						copier.ToStream(firstPage.Base, dataPager.PageSize * 2, dataStream);

						// txw.Commit(); intentionally not committing
					}

					// now can copy everything else
					var firstDataPage = dataPager.Read(2);

					copier.ToStream(firstDataPage.Base, dataPager.PageSize * (allocatedPages - 2), dataStream);

					try
					{
						foreach (var journalFile in files)
						{
							var journalPart = package.CreatePart(
								new Uri("/" + StorageEnvironmentOptions.LogName(journalFile.Number), UriKind.Relative),
								System.Net.Mime.MediaTypeNames.Application.Octet, 
								compression);

							Debug.Assert(journalPart != null);

							var pagesToCopy = journalFile.Pager.NumberOfAllocatedPages;
							if (journalFile.Number == lastWrittenLogFile)
								pagesToCopy = lastWrittenLogPage + 1;
							copier.ToStream(journalFile.Pager.Read(0).Base,
								pagesToCopy*journalFile.Pager.PageSize,
								journalPart.GetStream());
						}
					}
					finally
					{
						files.ForEach(x => x.Release());
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