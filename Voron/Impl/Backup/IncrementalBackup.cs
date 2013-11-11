// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackup.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.IO.Packaging;
using System.Linq;
using Voron.Impl.Journal;
using Voron.Util;

namespace Voron.Impl.Backup
{
	public unsafe class IncrementalBackup
	{
		public void ToFile(StorageEnvironment env, string backupPath)
		{
			if (env.Options.IncrementalBackupEnabled == false)
				throw new InvalidOperationException("Incremental backup is disabled for this storage");

			var backupInfo = env.BackupInfo;

			using (var package = Package.Open(backupPath, FileMode.Create))
			{
				//TODO LastBackedUpJournal lub LastBackedUpJournal + 1
				for (var journalNum = backupInfo.LastBackedUpJournal; journalNum <= backupInfo.LastCreatedJournal; journalNum++)
				{
					var part = package.CreatePart(new Uri("/" + env.Journal.LogName(journalNum), UriKind.Relative),
														 System.Net.Mime.MediaTypeNames.Application.Octet);

					var journalFile = env.Journal.Files.Find(x => x.Number == journalNum); // first check if it is a journal file in use
					if (journalFile == null)
					{
						journalFile = new JournalFile(env.Options.CreateJournalPager(env.Journal.LogName(journalNum)), journalNum);
						journalFile.DeleteOnClose(); // we can remove the journal after backup because it's not in use
					}

					journalFile.AddRef();
					try
					{
						var bufferSize = journalFile.Pager.PageSize * 16;

						//TODO Read(0)
						
						DataCopyHelper.ToStream(journalFile.Pager.Read(0).Base, env.Options.MaxLogFileSize, bufferSize, part.GetStream());
					}
					finally
					{
						journalFile.Release();
					}
				}
			}

			//TODO update incremental backup info
		}

		public void Restore(StorageEnvironment env, string backupPath)
		{
			throw new NotImplementedException();

			//using (var txw = env.NewTransaction(TransactionFlags.ReadWrite))
			//{
			//	env.FlushLogToDataFile();
			//	env._journal.Dispose();
			//	env._journal = new WriteAheadJournal(env);

			//	List<string> journalNames;

			//	using (var package = Package.Open(backupPath, FileMode.Open))
			//	{
			//		journalNames = package.GetParts().Select(x => x.Uri.OriginalString.Trim('/')).ToList();
			//	}

			//	var tempDir = Directory.CreateDirectory(Path.GetTempPath() + Guid.NewGuid()).FullName;

			//	try
			//	{
			//		ZipFile.ExtractToDirectory(backupPath, tempDir);

			//		TransactionHeader* lastTxHeader = null;

			//		var pagesToWrite = new Dictionary<long, JournalFile>();
					
			//		foreach (var journalName in journalNames)
			//		{
			//			var pager = new MemoryMapPager(Path.Combine(tempDir, journalName));
			//			long number;
						
			//			if (long.TryParse(journalName.Replace(".journal", string.Empty), out number) == false)
			//			{
			//				throw new InvalidOperationException("Cannot parse journal file number");
			//			}

			//			var file = new JournalFile(pager, number);

			//			lastTxHeader = file.RecoverAndValidate(0, lastTxHeader);

			//			foreach (var pageNumber in reader.TransactionPageTranslation.Keys)
			//			{
			//				pagesToWrite[pageNumber] = file;
			//			}
			//		}

			//		var sortedPages = pagesToWrite.OrderBy(x => x.Key)
			//									  .Select(x => x.Value.ReadPage(null, x.Key))
			//									  .ToList();

			//		var last = sortedPages.Last();

			//		env.Options.DataPager.EnsureContinuous(null, last.PageNumber,
			//										last.IsOverflow
			//											? env.Options.DataPager.GetNumberOfOverflowPages(
			//												last.OverflowSize)
			//											: 1);

			//		foreach (var page in sortedPages)
			//		{
			//			env.Options.DataPager.Write(page);
			//		}

			//		env.Options.DataPager.Sync();

			//		//env.SetStateAfterTransactionCommit(new StorageEnvironmentState()
			//		//	{
							
			//		//	}); 
			//	}
			//	finally
			//	{
			//		Directory.Delete(tempDir, true);
			//	}

			//	// txw.Commit(); no need to commit that
			//}
		}
	}
}