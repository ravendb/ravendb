// -----------------------------------------------------------------------
//  <copyright file="WriteAheadJournal.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Security;
using System.Threading;
using System.Threading.Tasks;
using Voron.Impl.FileHeaders;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public unsafe class WriteAheadJournal : IDisposable
	{
		private readonly StorageEnvironment _env;
		private readonly IVirtualPager _dataPager;

		private long _currentJournalFileSize;
		private DateTime _lastFile;

		private long _journalIndex = -1;
		private readonly SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1, 1);

		internal ImmutableList<JournalFile> Files = ImmutableList<JournalFile>.Empty;
		internal JournalFile CurrentFile;

		private HeaderAccessor _headerAccessor;

		public WriteAheadJournal(StorageEnvironment env)
		{
			_env = env;
			_dataPager = _env.Options.DataPager;
			_currentJournalFileSize = env.Options.InitialLogFileSize;
			_headerAccessor = env.HeaderAccessor;
		}

		private JournalFile NextFile(Transaction tx, int numberOfPages = 1)
		{
			_journalIndex++;


			var now = DateTime.UtcNow;
			if ((now - _lastFile).TotalSeconds < 90)
			{
				_currentJournalFileSize = Math.Min(_env.Options.MaxLogFileSize, _currentJournalFileSize * 2);
			}
			var actualLogSize = _currentJournalFileSize;
			var minRequiredsize = numberOfPages * AbstractPager.PageSize;
			if (_currentJournalFileSize < minRequiredsize)
			{
				actualLogSize = minRequiredsize;
			}
			_lastFile = now;

			var journalPager = _env.Options.CreateJournalWriter(_journalIndex, actualLogSize);

			var journal = new JournalFile(journalPager, _journalIndex);
			journal.AddRef(); // one reference added by a creator - write ahead log
			tx.SetLogReference(journal); // and the next one for the current transaction


			Files = Files.Add(journal);

			UpdateLogInfo();

			return journal;
		}

		public bool RecoverDatabase(TransactionHeader* txHeader)
		{
			// note, we don't need to do any concurrency here, happens as a single threaded
			// fashion on db startup
			var requireHeaderUpdate = false;

			var logInfo = _headerAccessor.Get(ptr => ptr->Journal);

			if (logInfo.JournalFilesCount == 0)
			{
				return false;
			}

			var oldestLogFileStillInUse = logInfo.CurrentJournal - logInfo.JournalFilesCount + 1;
			if (_env.Options.IncrementalBackupEnabled == false)
			{
				// we want to check that we cleanup old log files if they aren't needed
				// this is more just to be safe than anything else, they shouldn't be there.
				var unusedfiles = oldestLogFileStillInUse;
				while (true)
				{
					unusedfiles--;
					if (_env.Options.TryDeleteJournal(unusedfiles) == false)
						break;
				}

			}

			for (var journalNumber = oldestLogFileStillInUse; journalNumber <= logInfo.CurrentJournal; journalNumber++)
			{
				using (var pager = _env.Options.OpenJournalPager(journalNumber))
				{
					RecoverCurrentJournalSize(pager);

					long startRead = 0;

					if (journalNumber == logInfo.LastSyncedJournal)
						startRead = logInfo.LastSyncedJournalPage + 1;

					var transactionHeader = txHeader->TransactionId == 0 ? null : txHeader;
					var journalReader = new JournalReader(pager, startRead, transactionHeader);
					journalReader.RecoverAndValidate();

					// after reading all the pages from the journal file, we need to move them to the scratch buffers.
					var ptt = ImmutableDictionary<long, JournalFile.PagePosition>.Empty;
					foreach (var kvp in journalReader.TransactionPageTranslation)
					{
						var page = pager.Read(kvp.Value.JournalPos);
						var numOfPages = page.IsOverflow ? pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;
						var scratchBuffer = _env.ScratchBufferPool.Allocate(null, numOfPages);

						NativeMethods.memcpy(scratchBuffer.Pointer, page.Base, numOfPages * AbstractPager.PageSize);

						ptt = ptt.SetItem(kvp.Key, new JournalFile.PagePosition
						{
							ScratchPos = scratchBuffer.PositionInScratchBuffer,
							JournalPos = kvp.Value.JournalPos,
							TransactionId = kvp.Value.TransactionId
						});
					}

					// we setup the journal file so we can flush from it to the data file
					var jrnlWriter = _env.Options.CreateJournalWriter(journalNumber, pager.NumberOfAllocatedPages * AbstractPager.PageSize);
					var jrnlFile = new JournalFile(jrnlWriter, journalNumber);
					jrnlFile.InitFrom(journalReader, ptt);
					jrnlFile.AddRef(); // creator reference - write ahead log
					Files = Files.Add(jrnlFile);

					*txHeader = *journalReader.LastTransactionHeader;

					if (journalReader.RequireHeaderUpdate) //this should prevent further loading of transactions
					{
						requireHeaderUpdate = true;
						break;
					}
				}
			}

			if (requireHeaderUpdate)
			{
				_headerAccessor.Modify(header =>
					{
						header->Journal.CurrentJournal = Files.Count - 1;
					});

				logInfo = _headerAccessor.Get(ptr => ptr->Journal);

				// we want to check that we cleanup newer log files, since everything from
				// the current file is considered corrupted
				var badJournalFiles = logInfo.CurrentJournal;
				while (true)
				{
					badJournalFiles++;
					if (_env.Options.TryDeleteJournal(badJournalFiles) == false)
						break;
				}
			}

			_journalIndex = logInfo.CurrentJournal;

			if (Files.IsEmpty == false)
			{
				var lastFile = Files.Last();
				if (lastFile.AvailablePages >= 2)
					// it must have at least one page for the next transaction header and one page for data
					CurrentFile = lastFile;
			}

			if (requireHeaderUpdate)
			{
				UpdateLogInfo();
			}
			return requireHeaderUpdate;
		}

		private void RecoverCurrentJournalSize(IVirtualPager pager)
		{
			var journalSize = Utils.NearestPowerOfTwo(pager.NumberOfAllocatedPages * AbstractPager.PageSize);
			if (journalSize >= _env.Options.MaxLogFileSize) // can't set for more than the max log file size
				return;

			_currentJournalFileSize = journalSize;
		}

		public void UpdateLogInfo()
		{
			_headerAccessor.Modify(header =>
				{
					header->Journal.CurrentJournal = Files.Count > 0 ? _journalIndex : -1;
					header->Journal.JournalFilesCount = Files.Count;
					header->IncrementalBackup.LastCreatedJournal = _journalIndex;
				});
		}

		public Page ReadPage(Transaction tx, long pageNumber)
		{
			// read transactions have to read from log snapshots
			if (tx.Flags == TransactionFlags.Read)
			{
				// read log snapshots from the back to get the most recent version of a page
				for (var i = tx.JournalSnapshots.Count - 1; i >= 0; i--)
				{
					JournalFile.PagePosition value;
					if (tx.JournalSnapshots[i].PageTranslationTable.TryGetValue(pageNumber, out value))
						return _env.ScratchBufferPool.ReadPage(value.ScratchPos);
				}

				return null;
			}

			// write transactions can read directly from logs
			for (var i = Files.Count - 1; i >= 0; i--)
			{
				JournalFile.PagePosition value;
				if (Files[i].PageTranslationTable.TryGetValue(pageNumber, out value))
					return _env.ScratchBufferPool.ReadPage(value.ScratchPos);
			}

			return null;
		}

		public void Dispose()
		{
			if (_env.Options.OwnsPagers)
			{
				foreach (var logFile in Files)
				{
					logFile.Dispose();
				}
			}
			else
			{
				foreach (var logFile in Files)
				{
					GC.SuppressFinalize(logFile);
				}

			}

			Files.Clear();
		}

		public JournalInfo GetCurrentJournalInfo()
		{
			return _headerAccessor.Get(ptr => ptr->Journal);
		}

		public List<JournalSnapshot> GetSnapshots()
		{
			return Files.Select(x => x.GetSnapshot()).ToList();
		}

		public long SizeOfUnflushedTransactionsInJournalFile()
		{
			using (var tx = _env.NewTransaction(TransactionFlags.Read))
			{
				var journalInfo = _headerAccessor.Get(ptr => ptr->Journal);

				var lastSyncedLog = journalInfo.LastSyncedJournal;
				var lastSyncedLogPage = journalInfo.LastSyncedJournalPage;

				var sum = Files.Sum(file =>
				{
					if (file.Number == lastSyncedLog && lastSyncedLog != 0)
						return lastSyncedLogPage - file.WritePagePosition - 1;
					return file.WritePagePosition == 0 ? 0 : file.WritePagePosition - 1;
				});

				tx.Commit();
				return sum;
			}
		}

		public class JournalApplicator
		{
			private readonly WriteAheadJournal _waj;
			private readonly long _oldestActiveTransaction;
			private long _lastSyncedJournal;
			private long _lastSyncedPage;
			private List<JournalSnapshot> _jrnls;

			public JournalApplicator(WriteAheadJournal waj, long oldestActiveTransaction)
			{
				_waj = waj;
				_oldestActiveTransaction = oldestActiveTransaction;
			}


			public void ApplyLogsToDataFile()
			{
				using (var tx = _waj._env.NewTransaction(TransactionFlags.ReadWrite))
				{
					_jrnls = _waj.Files.Select(x => x.GetSnapshot()).ToList();
					if (_jrnls.Count == 0)
						return; // nothing to do

					var journalInfo = _waj._headerAccessor.Get(ptr => ptr->Journal);

					_lastSyncedJournal = journalInfo.LastSyncedJournal;
					_lastSyncedPage = journalInfo.LastSyncedJournalPage;
					Debug.Assert(_jrnls.First().Number >= _lastSyncedJournal);

					tx.Commit();
				}


				var pagesToWrite = ImmutableDictionary<long, long>.Empty;
				foreach (var journalFile in _jrnls)
				{
					if (journalFile.PageTranslationTable.Count == 0)
						continue;
					_lastSyncedJournal = Math.Max(journalFile.Number, _lastSyncedJournal);
					_lastSyncedPage = 0;
					foreach (var pagePosition in journalFile.PageTranslationTable)
					{
						if (_oldestActiveTransaction != 0 &&
							pagePosition.Value.TransactionId >= _oldestActiveTransaction)
						{
							// we cannot write this yet, there is a read transaction that might be looking at this
							// however, we _aren't_ going to be writing this to the data file, since that would be a 
							// waste, we would just overwrite that value in the next flush anyway
							pagesToWrite = pagesToWrite.Remove(pagePosition.Key);
							continue;
						}
						_lastSyncedPage = Math.Max(_lastSyncedPage, pagePosition.Value.JournalPos);
					}
				}

				if (pagesToWrite.Count == 0)
					return;

				var scratchBufferPool = _waj._env.ScratchBufferPool;
				var sortedPages = pagesToWrite.OrderBy(x => x.Key)
											  .Select(x => scratchBufferPool.ReadPage(x.Value))
											  .ToList();

				var last = sortedPages.Last();

				var lastPage = last.IsOverflow == false ? 1 :
					_waj._env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize);

				_waj._dataPager.EnsureContinuous(null, last.PageNumber, lastPage);

				foreach (var page in sortedPages)
				{
					_waj._dataPager.Write(page);
				}

				var unusedJournalFiles = GetUnusedJournalFiles();

				using (var txw = _waj._env.NewTransaction(TransactionFlags.ReadWrite))
				{
					var journalFile = _waj.Files.First(x => x.Number == _lastSyncedJournal);
					UpdateFileHeaderAfterDataFileSync(journalFile);

					var lastJournalFileToRemove = unusedJournalFiles.LastOrDefault();
					if (lastJournalFileToRemove != null)
						_waj.Files = _waj.Files.RemoveAll(x => x.Number <= lastJournalFileToRemove.Number);

					_waj.UpdateLogInfo();

					if (_waj.Files.Count == 0)
					{
						_waj.CurrentFile = null;
					}

					FreeScratchPages(unusedJournalFiles);

					foreach (var fullLog in unusedJournalFiles)
					{
						fullLog.Release();
					}

					txw.Commit();
				}

			}

			private void FreeScratchPages(IEnumerable<JournalFile> unusedJournalFiles)
			{
				foreach (var jrnl in _waj.Files)
				{
					jrnl.FreeScratchPagesOlderThan(_waj._env, _oldestActiveTransaction);
				}
				foreach (var journalFile in unusedJournalFiles)
				{
					if (_waj._env.Options.IncrementalBackupEnabled == false)
						journalFile.DeleteOnClose = true;
					journalFile.FreeScratchPagesOlderThan(_waj._env, long.MaxValue);
				}
			}

			private List<JournalFile> GetUnusedJournalFiles()
			{
				var unusedJournalFiles = new List<JournalFile>();
				foreach (var j in _waj.Files)
				{
					if (j.Number > _lastSyncedJournal) // after the last log we synced, nothing to do here
						continue;
					if (j.Number == _lastSyncedJournal) // we are in the last log we synced
					{
						// if we didn't get to end, or if there are more pages to be used here, ignore it
						if (j.AvailablePages != 0)
							continue;
					}
					unusedJournalFiles.Add(j);
				}
				return unusedJournalFiles;
			}

			public void UpdateFileHeaderAfterDataFileSync(JournalFile file)
			{
				var txHeader = stackalloc TransactionHeader[1];
				txHeader->TransactionId = 0;
				var txPos = 0;
				while (true)
				{
					file.ReadTransaction(txPos, txHeader);
					if (txHeader->TransactionId + 1 == _oldestActiveTransaction)
						break;
					txPos += txHeader->PageCount + txHeader->OverflowPageCount + 1;
				}

				_waj._headerAccessor.Modify(header =>
					{
						header->TransactionId = txHeader->TransactionId;
						header->LastPageNumber = txHeader->LastPageNumber;

						header->Journal.LastSyncedJournal = _lastSyncedJournal;
						header->Journal.LastSyncedJournalPage = _lastSyncedPage == 0 ? -1 : _lastSyncedPage;

						header->Root = txHeader->Root;
						header->FreeSpace = txHeader->FreeSpace;
					});
			}
		}

		public Task WriteToJournal(Transaction tx, int pageCount)
		{
			// this is a bit strange, because we want to return a task of the actual write to disk
			// but at the same time, we want to only allow a single write to disk at a given point in time
			// there for, we wait for the write to disk to complete before releasing the semaphore
			_writeSemaphore.Wait();
			try
			{
				if (CurrentFile == null || CurrentFile.AvailablePages < pageCount)
				{
					CurrentFile = NextFile(tx, pageCount);
				}
				var task = CurrentFile.Write(tx, pageCount)
					.ContinueWith(result =>
					{
						_writeSemaphore.Release(); // release semaphore on write completion
						return result;
					}).Unwrap();
				if (CurrentFile.AvailablePages == 0)
				{
					CurrentFile = null;
				}

				return task;
			}
			catch
			{
				_writeSemaphore.Release();
				throw;
			}
		}
	}
}
