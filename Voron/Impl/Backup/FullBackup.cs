// -----------------------------------------------------------------------
//  <copyright file="FullBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.IO.Compression;
using System.IO.Packaging;
using Voron.Util;

namespace Voron.Impl.Backup
{
	public unsafe class FullBackup
	{
		public void ToFile(StorageEnvironment env, string backupPath)
		{
			var dataPager = env.Options.DataPager;
			var bufferSize = dataPager.PageSize * 16;

			Transaction txr = null;
			try
			{
				using (var package = Package.Open(backupPath, FileMode.Create))
				{
					// data file backup
					var dataPart = package.CreatePart(new Uri("/db.voron", UriKind.Relative),
													  System.Net.Mime.MediaTypeNames.Application.Octet);

					var dataStream = dataPart.GetStream();

					long allocatedPages;

					using (var txw = env.NewTransaction(TransactionFlags.ReadWrite)) // so we can snapshot the headers safely
					{
						txr = env.NewTransaction(TransactionFlags.Read); // now have snapshot view
						allocatedPages = dataPager.NumberOfAllocatedPages;

						var firstPage = dataPager.Read(0);


						DataCopyHelper.ToStream(firstPage.Base, dataPager.PageSize * 2, bufferSize, dataStream);

						//txw.Commit(); intentionally not committing
					}

					// now can copy everything else
					var firstDataPage = dataPager.Read(2);

					DataCopyHelper.ToStream(firstDataPage.Base, dataPager.PageSize * (allocatedPages - 2), bufferSize, dataStream);

					// log files backup

					var files = env.Journal.Files; // thread safety copy

					files.ForEach(x => x.AddRef());

					try
					{
						foreach (var journalFile in files)
						{
							var journalPart = package.CreatePart(
								new Uri("/" + env.Journal.LogName(journalFile.Number), UriKind.Relative),
								System.Net.Mime.MediaTypeNames.Application.Octet);

							DataCopyHelper.ToStream(journalFile.Pager.Read(0).Base, env.Options.MaxLogFileSize, bufferSize,
							                        journalPart.GetStream());
						}
					}
					finally
					{
						files.ForEach(x => x.Release());
					}

					//txr.Commit(); intentionally not committing
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