// -----------------------------------------------------------------------
//  <copyright file="WriteAheadJournal.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
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

		private readonly SemaphoreSlim _flushingSemaphore = new SemaphoreSlim(1, 1);
		private SafeList<JournalFile> _files = SafeList<JournalFile>.Empty;
		internal JournalFile CurrentFile;

		private readonly HeaderAccessor _headerAccessor;
		private long _lastFlushedTransaction = -1;

		public WriteAheadJournal(StorageEnvironment env)
		{
			_env = env;
			_dataPager = _env.Options.DataPager;
			_currentJournalFileSize = env.Options.InitialLogFileSize;
			_headerAccessor = env.HeaderAccessor;
		}

		public SafeList<JournalFile> Files { get { return _files; }}

		private JournalFile NextFile(int numberOfPages = 1)
		{
			_journalIndex++;

			var now = DateTime.UtcNow;
			if ((now - _lastFile).TotalSeconds < 90)
			{
				_currentJournalFileSize = Math.Min(_env.Options.MaxLogFileSize, _currentJournalFileSize * 2);
			}
			var actualLogSize = _currentJournalFileSize;
			var minRequiredSize = numberOfPages * AbstractPager.PageSize;
			if (_currentJournalFileSize < minRequiredSize)
			{
				actualLogSize = minRequiredSize;
			}
			_lastFile = now;

			var journalPager = _env.Options.CreateJournalWriter(_journalIndex, actualLogSize);

			var journal = new JournalFile(journalPager, _journalIndex);
			journal.AddRef(); // one reference added by a creator - write ahead log

			_files = _files.Add(journal);

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

			var journalFiles = new List<JournalFile>();
		    long journalNumber;
			for (journalNumber = oldestLogFileStillInUse; journalNumber <= logInfo.CurrentJournal; journalNumber++)
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
					var ptt = new Dictionary<long, JournalFile.PagePosition>();
					foreach (var kvp in journalReader.TransactionPageTranslation)
					{
						var page = pager.Read(kvp.Value.JournalPos);
						var numOfPages = page.IsOverflow ? pager.GetNumberOfOverflowPages(page.OverflowSize) : 1;
						var scratchBuffer = _env.ScratchBufferPool.Allocate(null, numOfPages);
						var scratchPage = _env.ScratchBufferPool.ReadPage(scratchBuffer.PositionInScratchBuffer);
						NativeMethods.memcpy(scratchPage.Base, page.Base, numOfPages * AbstractPager.PageSize);

						ptt[kvp.Key] = new JournalFile.PagePosition
						{
							ScratchPos = scratchBuffer.PositionInScratchBuffer,
							JournalPos = kvp.Value.JournalPos,
							TransactionId = kvp.Value.TransactionId
						};
					}


					// we setup the journal file so we can flush from it to the data file
					var jrnlWriter = _env.Options.CreateJournalWriter(journalNumber, pager.NumberOfAllocatedPages * AbstractPager.PageSize);
					var jrnlFile = new JournalFile(jrnlWriter, journalNumber);
					jrnlFile.InitFrom(journalReader, SafeDictionary<long, JournalFile.PagePosition>.From(ptt), journalReader.TransactionEndPositions);
					jrnlFile.AddRef(); // creator reference - write ahead log

					journalFiles.Add(jrnlFile);

					var lastReadHeaderPtr = journalReader.LastTransactionHeader;

					if (lastReadHeaderPtr != null)
						*txHeader = *lastReadHeaderPtr;

					if (journalReader.RequireHeaderUpdate) //this should prevent further loading of transactions
					{
						requireHeaderUpdate = true;
						break;
					}
				}


			}
			_files = _files.AddRange(journalFiles);

			if (requireHeaderUpdate)
			{
				_headerAccessor.Modify(header =>
					{
				        header->Journal.CurrentJournal = journalNumber;
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

			if (_files.Count > 0)
			{
				var lastFile = _files.Last();
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
					var journalFilesCount = _files.Count;
					header->Journal.CurrentJournal = journalFilesCount > 0 ? _journalIndex : -1;
					header->Journal.JournalFilesCount = journalFilesCount;
					header->IncrementalBackup.LastCreatedJournal = _journalIndex;
				});
		}

		public Page ReadPage(Transaction tx, long pageNumber)
		{
			// read transactions have to read from journal snapshots
			if (tx.Flags == TransactionFlags.Read)
			{
				// read log snapshots from the back to get the most recent version of a page
				for (var i = tx.JournalSnapshots.Count - 1; i >= 0; i--)
				{
					JournalFile.PagePosition value;
					if (tx.JournalSnapshots[i].PageTranslationTable.TryGetValue(pageNumber, out value))
					{
						if (value.TransactionId <= _lastFlushedTransaction)
						{
							// requested page is already in the data file, don't read from the scratch space 
							// because it was freed and might be overwritten there
							return null;
				}

						var page = _env.ScratchBufferPool.ReadPage(value.ScratchPos);

						Debug.Assert(page.PageNumber == pageNumber);

						return page;
					}
				}

				return null;
			}

			// write transactions can read directly from journals
			var files = _files;
			for (var i = files.Count - 1; i >= 0; i--)
			{
				JournalFile.PagePosition value;
				if (files[i].PageTranslationTable.TryGetValue(pageNumber, out value))
				{
					var page = _env.ScratchBufferPool.ReadPage(value.ScratchPos);

					Debug.Assert(page.PageNumber == pageNumber);

					return page;
			}
			}

			return null;
		}

		private bool disposed;

		public void Dispose()
		{
			if (disposed)
				return;
			disposed = true;

			// we cannot dispose the journal until we are done with all of the pending writes

			_writeSemaphore.Wait();

			if (_env.Options.OwnsPagers)
			{
				foreach (var logFile in _files)
				{
					logFile.Dispose();
				}
			}
			else
			{
				foreach (var logFile in _files)
				{
					GC.SuppressFinalize(logFile);
				}

			}

			_files = SafeList<JournalFile>.Empty;

			_writeSemaphore.Dispose();
		}

		public JournalInfo GetCurrentJournalInfo()
		{
			return _headerAccessor.Get(ptr => ptr->Journal);
		}

		public List<JournalSnapshot> GetSnapshots()
		{
			return _files.Select(x => x.GetSnapshot()).ToList();
		}

		public long SizeOfUnflushedTransactionsInJournalFile()
		{
			using (var tx = _env.NewTransaction(TransactionFlags.Read))
			{
				var journalInfo = _headerAccessor.Get(ptr => ptr->Journal);

				var lastSyncedLog = journalInfo.LastSyncedJournal;
				var lastSyncedLogPage = journalInfo.LastSyncedJournalPage;

				var sum = _files.Sum(file =>
				{
					if (file.Number == lastSyncedLog && lastSyncedLog != 0)
						return lastSyncedLogPage - file.WritePagePosition - 1;
					return file.WritePagePosition == 0 ? 0 : file.WritePagePosition - 1;
				});

				tx.Commit();
				return sum;
			}
		}

		public void Clear(Transaction tx)
		{
			if(tx.Flags != TransactionFlags.ReadWrite)
				throw new InvalidOperationException("Clearing of write ahead journal should be called only from a write transaction");

			_files.ForEach(x => x.Release());
			_files = SafeList<JournalFile>.Empty;
			CurrentFile = null;
		}

		public class JournalApplicator : IDisposable
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
				_waj._flushingSemaphore.WaitAsync();
			}

			public void ApplyLogsToDataFile(Transaction transaction = null)
			{
                var alreadyInWriteTx = transaction != null && transaction.Flags == TransactionFlags.ReadWrite;

				var journalFiles = _waj._files;
                _jrnls = journalFiles.Select(x => x.GetSnapshot()).OrderBy(x => x.Number).ToList();
                if (_jrnls.Count == 0)
                    return; // nothing to do

                var journalInfo = _waj._headerAccessor.Get(ptr => ptr->Journal);

                _lastSyncedJournal = journalInfo.LastSyncedJournal;
                _lastSyncedPage = journalInfo.LastSyncedJournalPage;
                Debug.Assert(_jrnls.First().Number >= _lastSyncedJournal);

				var pagesToWrite = new Dictionary<long, long>();

				long lastProcessedJournal = -1;
				long previousJournalMaxTransactionId = -1;

				long lastFlushedTransactionId = -1;

			    foreach (var journalFile in _jrnls.Where(x => x.Number >= _lastSyncedJournal))
                {
                    if (journalFile.PageTranslationTable.Count == 0)
                        continue;

	                var currentJournalMaxTransactionId = -1L;

                    foreach (var pagePosition in journalFile.PageTranslationTable)
                    {
                        if (_oldestActiveTransaction != 0 &&
                            pagePosition.Value.TransactionId >= _oldestActiveTransaction)
                        {
                            // we cannot write this yet, there is a read transaction that might be looking at this
                            // however, we _aren't_ going to be writing this to the data file, since that would be a 
                            // waste, we would just overwrite that value in the next flush anyway
                            pagesToWrite.Remove(pagePosition.Key);
                            continue;
                        }

						if(journalFile.Number == _lastSyncedJournal && pagePosition.Value.JournalPos <= _lastSyncedPage)
							continue;

	                    currentJournalMaxTransactionId = Math.Max(currentJournalMaxTransactionId, pagePosition.Value.TransactionId);

	                    if (currentJournalMaxTransactionId < previousJournalMaxTransactionId)
		                    throw new InvalidOperationException(
			                    "Journal applicator read beyond the oldest active transaction in the next journal file. " +
			                    "This should never happen. Current journal max tx id: " + currentJournalMaxTransactionId +
			                    ", previous journal max ix id: " + previousJournalMaxTransactionId +
			                    ", oldest active transaction: " + _oldestActiveTransaction);


	                    lastProcessedJournal = journalFile.Number;
                        pagesToWrite[pagePosition.Key] =  pagePosition.Value.ScratchPos;

						lastFlushedTransactionId = currentJournalMaxTransactionId;
                    }

	                previousJournalMaxTransactionId = currentJournalMaxTransactionId;
                }

                if (pagesToWrite.Count == 0)
                    return;

				_lastSyncedJournal = lastProcessedJournal;
				_lastSyncedPage = _jrnls.First(x => x.Number == _lastSyncedJournal).TransactionEndPositions[lastFlushedTransactionId];

                var scratchBufferPool = _waj._env.ScratchBufferPool;
				var scratchPagerState = scratchBufferPool.PagerState;
				scratchPagerState.AddRef();

				try
				{
                    var sortedPages = pagesToWrite.OrderBy(x => x.Key)
                                                    .Select(x => scratchBufferPool.ReadPage(x.Value))
                                                    .ToList();

                    var last = sortedPages.Last();

                    var numberOfPagesInLastPage = last.IsOverflow == false ? 1 :
                        _waj._env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize);

					DebugValidateWrittenTransaction(lastFlushedTransactionId);

					EnsureDataPagerSpacing(transaction, last, numberOfPagesInLastPage, alreadyInWriteTx);

			        foreach (var page in sortedPages)
			        {
			            _waj._dataPager.Write(page);
			        }

                    _waj._dataPager.Sync();
				}
				finally 
				{
					scratchPagerState.Release();
				}

				var unusedJournalFiles = GetUnusedJournalFiles();

                using (var txw = alreadyInWriteTx ? null : _waj._env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var journalFile = journalFiles.First(x => x.Number == _lastSyncedJournal);
                    UpdateFileHeaderAfterDataFileSync(journalFile);

                    var lastJournalFileToRemove = unusedJournalFiles.LastOrDefault();
                    if (lastJournalFileToRemove != null)
						_waj._files = _waj._files.RemoveAll(x => x.Number <= lastJournalFileToRemove.Number);

                    _waj.UpdateLogInfo();

					if (_waj._files.Count == 0)
                    {
                        _waj.CurrentFile = null;
                    }

					Debug.Assert(lastFlushedTransactionId != -1);

	                _waj._lastFlushedTransaction = lastFlushedTransactionId;

                    FreeScratchPages(unusedJournalFiles);

                    foreach (var fullLog in unusedJournalFiles)
                    {
                        fullLog.Release();
                    }

                    if (txw != null)
                        txw.Commit();
                }
			}

			private void EnsureDataPagerSpacing(Transaction transaction, Page last, int numberOfPagesInLastPage,
				bool alreadyInWriteTx)
			{
				if (_waj._dataPager.WillRequireExtension(last.PageNumber, numberOfPagesInLastPage))
				{
					if (alreadyInWriteTx)
					{
						_waj._dataPager.EnsureContinuous(transaction, last.PageNumber, numberOfPagesInLastPage);
					}
					else
					{
						using (var tx = _waj._env.NewTransaction(TransactionFlags.ReadWrite))
						{
							_waj._dataPager.EnsureContinuous(tx, last.PageNumber, numberOfPagesInLastPage);

							tx.Commit();
						}
					}
				}
			}

			[Conditional("DEBUG")]
			private void DebugValidateWrittenTransaction(long lastFlushedTransactionId)
			{
                var txHeaders = stackalloc TransactionHeader[1];
                var readTxHeader = &txHeaders[0];
                _waj._writeSemaphore.Wait();
                try
                {
                    var jrnl = _waj._files.First(x => x.Number == _lastSyncedJournal);
                    var read = jrnl.ReadTransaction(_lastSyncedPage + 1, readTxHeader);

                    if (readTxHeader->HeaderMarker != 0 && (read == false || readTxHeader->HeaderMarker != Constants.TransactionHeaderMarker || readTxHeader->TransactionId != lastFlushedTransactionId + 1))
                    {
                        var start = 0;
                        var positions = new Dictionary<long, TransactionHeader>();
                        while (jrnl.ReadTransaction(start, readTxHeader))
                        {
                            positions.Add(start, *readTxHeader);
                            start += readTxHeader->PageCount + readTxHeader->OverflowPageCount + 1;
                        }

                        throw new InvalidOperationException(
                            "Reading transaction for calculated page in journal failed. "
                            + "Journal #" + _lastSyncedJournal + ". "
                            + "Page #" + (_lastSyncedPage + 1) + ". "
                            + "This means that page calculation is wrong and should be fixed otherwise data will be lost during database recovery from this journal.");
                    }
                }
                finally
                {
                    _waj._writeSemaphore.Release();
                }
			}

			private void FreeScratchPages(IEnumerable<JournalFile> unusedJournalFiles)
			{
				foreach (var jrnl in _waj._files)
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
				foreach (var j in _jrnls)
				{
					if (j.Number > _lastSyncedJournal) // after the last log we synced, nothing to do here
						continue;
					if (j.Number == _lastSyncedJournal) // we are in the last log we synced
					{
                        if (j.AvailablePages != 0 || //　if there are more pages to be used here or 
                        j.PageTranslationTable.Max(x => x.Value.JournalPos) != _lastSyncedPage) // we didn't synchronize whole journal
                            continue; // do not mark it as unused
					}
					unusedJournalFiles.Add(_waj._files.First(x => x.Number == j.Number));
				}
				return unusedJournalFiles;
			}

			public void UpdateFileHeaderAfterDataFileSync(JournalFile file)
			{
				var txHeaders = stackalloc TransactionHeader[2];
				var readTxHeader = &txHeaders[0];
				var lastReadTxHeader = txHeaders[1];

				var txPos = 0;
				while (true)
				{
					if (file.ReadTransaction(txPos, readTxHeader) == false)
						break;
					if(readTxHeader->HeaderMarker != Constants.TransactionHeaderMarker)
						break;
					if (readTxHeader->TransactionId + 1 == _oldestActiveTransaction)
						break;

					lastReadTxHeader = *readTxHeader;

					txPos += readTxHeader->PageCount + readTxHeader->OverflowPageCount + 1;
				}

				Debug.Assert(_lastSyncedJournal != -1);
				Debug.Assert(_lastSyncedPage != -1);

				_waj._headerAccessor.Modify(header =>
					{
						header->TransactionId = lastReadTxHeader.TransactionId;
						header->LastPageNumber = lastReadTxHeader.LastPageNumber;

						header->Journal.LastSyncedJournal = _lastSyncedJournal;
						header->Journal.LastSyncedJournalPage = _lastSyncedPage;

						header->Root = lastReadTxHeader.Root;
						header->FreeSpace = lastReadTxHeader.FreeSpace;
					});
			}

			public void Dispose()
			{
				_waj._flushingSemaphore.Release();
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
					CurrentFile = NextFile(pageCount);
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
