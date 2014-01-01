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
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Journal
{
	using System.IO;

	public unsafe class WriteAheadJournal : IDisposable
	{
		private readonly StorageEnvironment _env;
		private readonly IVirtualPager _dataPager;

		private long _currentJournalFileSize;
		private DateTime _lastFile;

		private long _journalIndex = -1;
		private readonly LZ4 _lz4 = new LZ4();
		private readonly JournalApplicator _journalApplicator;
		private readonly ModifyHeaderAction _updateLogInfo;

		private ImmutableAppendOnlyList<JournalFile> _files = ImmutableAppendOnlyList<JournalFile>.Empty;
		internal JournalFile CurrentFile;

		private readonly HeaderAccessor _headerAccessor;
		private long _lastFlushedTransaction = -1;

		private IVirtualPager _compressionPager;

		public WriteAheadJournal(StorageEnvironment env)
		{
			_env = env;
			_dataPager = _env.Options.DataPager;
			_currentJournalFileSize = env.Options.InitialLogFileSize;
			_headerAccessor = env.HeaderAccessor;
			_updateLogInfo = header =>
			{
				var journalFilesCount = _files.Count;
				header->Journal.CurrentJournal = journalFilesCount > 0 ? _journalIndex : -1;
				header->Journal.JournalFilesCount = journalFilesCount;
				header->IncrementalBackup.LastCreatedJournal = _journalIndex;
			};

			_compressionPager = _env.Options.CreateScratchPager("compression.buffers");
			_journalApplicator = new JournalApplicator(this);
		}

		public ImmutableAppendOnlyList<JournalFile> Files { get { return _files; } }

		public JournalApplicator Applicator { get { return _journalApplicator; } }

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

			_files = _files.Append(journal);

			_headerAccessor.Modify(_updateLogInfo);

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

			var lastSyncedTransactionId = logInfo.LastSyncedTransactionId;

			var journalFiles = new List<JournalFile>();
			long lastSyncedTxId = -1;
			long lastSyncedJournal = logInfo.LastSyncedJournal;
			for (var journalNumber = oldestLogFileStillInUse; journalNumber <= logInfo.CurrentJournal; journalNumber++)
			{
				using (var recoveryPager = _env.Options.CreateScratchPager(StorageEnvironmentOptions.JournalRecoveryName(journalNumber)))
				using (var pager = _env.Options.OpenJournalPager(journalNumber))
				{
					RecoverCurrentJournalSize(pager);

					var transactionHeader = txHeader->TransactionId == 0 ? null : txHeader;
					var journalReader = new JournalReader(pager, recoveryPager, lastSyncedTransactionId, transactionHeader);
					journalReader.RecoverAndValidate(_env.Options);

					var pagesToWrite = journalReader
						.TransactionPageTranslation
						.Select(kvp => recoveryPager.Read(kvp.Value.JournalPos))
						.OrderBy(x => x.PageNumber)
						.ToList();

					var lastReadHeaderPtr = journalReader.LastTransactionHeader;

					if (lastReadHeaderPtr != null)
					{
						if (pagesToWrite.Count > 0)
							ApplyPagesToDataFileFromJournal(pagesToWrite);

						*txHeader = *lastReadHeaderPtr;
						lastSyncedTxId = txHeader->TransactionId;
						lastSyncedJournal = journalNumber;
					}

					if (journalReader.RequireHeaderUpdate || journalNumber == logInfo.CurrentJournal)
					{
						var jrnlWriter = _env.Options.CreateJournalWriter(journalNumber, pager.NumberOfAllocatedPages * AbstractPager.PageSize);
						var jrnlFile = new JournalFile(jrnlWriter, journalNumber);
						jrnlFile.InitFrom(journalReader);
						jrnlFile.AddRef(); // creator reference - write ahead log

						journalFiles.Add(jrnlFile);
					}

					if (journalReader.RequireHeaderUpdate) //this should prevent further loading of transactions
					{
						requireHeaderUpdate = true;
						break;
					}
				}
			}

			_files = _files.AppendRange(journalFiles);

			Debug.Assert(lastSyncedTxId >= 0);
			Debug.Assert(lastSyncedJournal >= 0);

			_journalIndex = lastSyncedJournal;

			_headerAccessor.Modify(
				header =>
				{
					header->Journal.LastSyncedJournal = lastSyncedJournal;
					header->Journal.LastSyncedTransactionId = lastSyncedTxId;
					header->Journal.CurrentJournal = lastSyncedJournal;
					header->Journal.JournalFilesCount = _files.Count;
					header->IncrementalBackup.LastCreatedJournal = _journalIndex;
				});

			CleanupInvalidJournalFiles(lastSyncedJournal);
			CleanupUnusedJournalFiles(oldestLogFileStillInUse, lastSyncedJournal);

			if (_files.Count > 0)
			{
				var lastFile = _files.Last();
				if (lastFile.AvailablePages >= 2)
					// it must have at least one page for the next transaction header and one page for data
					CurrentFile = lastFile;
			}

			return requireHeaderUpdate;
		}

		private void CleanupUnusedJournalFiles(long oldestLogFileStillInUse, long lastSyncedJournal)
		{
			var logFile = oldestLogFileStillInUse;
			while (logFile < lastSyncedJournal)
			{
				_env.Options.TryDeleteJournal(logFile);
				logFile++;
			}
		}

		private void CleanupInvalidJournalFiles(long lastSyncedJournal)
		{
			// we want to check that we cleanup newer log files, since everything from
			// the current file is considered corrupted
			var badJournalFiles = lastSyncedJournal;
			while (true)
			{
				badJournalFiles++;
				if (_env.Options.TryDeleteJournal(badJournalFiles) == false)
				{
					break;
				}
			}
		}

		private void ApplyPagesToDataFileFromJournal(List<Page> sortedPagesToWrite)
		{
			var last = sortedPagesToWrite.Last();

			var numberOfPagesInLastPage = last.IsOverflow == false ? 1 :
				_env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize);

			_dataPager.EnsureContinuous(null, last.PageNumber, numberOfPagesInLastPage);

			foreach (var page in sortedPagesToWrite)
			{
				_dataPager.Write(page);
			}

			_dataPager.Sync();
		}

		private void RecoverCurrentJournalSize(IVirtualPager pager)
		{
			var journalSize = Utils.NearestPowerOfTwo(pager.NumberOfAllocatedPages * AbstractPager.PageSize);
			if (journalSize >= _env.Options.MaxLogFileSize) // can't set for more than the max log file size
				return;

			_currentJournalFileSize = journalSize;
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
					if (tx.JournalSnapshots[i].PageTranslationTable.TryGetValue(tx, pageNumber, out value))
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
				if (files[i].PageTranslationTable.TryGetValue(tx, pageNumber, out value))
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

			_compressionPager.Dispose();
			_lz4.Dispose();
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

			_files = ImmutableAppendOnlyList<JournalFile>.Empty;
		}

		public JournalInfo GetCurrentJournalInfo()
		{
			return _headerAccessor.Get(ptr => ptr->Journal);
		}

		public List<JournalSnapshot> GetSnapshots()
		{
			return _files.Select(x => x.GetSnapshot()).ToList();
		}

		public void Clear(Transaction tx)
		{
			if (tx.Flags != TransactionFlags.ReadWrite)
				throw new InvalidOperationException("Clearing of write ahead journal should be called only from a write transaction");

			foreach (var journalFile in _files)
			{
				journalFile.Release();
			}
			_files = ImmutableAppendOnlyList<JournalFile>.Empty;
			CurrentFile = null;
		}

		public class JournalApplicator
		{
			private const long DelayedDataFileSynchronizationBytesLimit = 2L*1024*1024*1024;
			private readonly TimeSpan DelayedDataFileSynchronizationTimeLimit = TimeSpan.FromMinutes(1);
			private readonly SemaphoreSlim _flushingSemaphore = new SemaphoreSlim(1, 1);
			private readonly Dictionary<long, JournalFile> _journalsToDelete = new Dictionary<long, JournalFile>();
			private readonly WriteAheadJournal _waj;
			private long _lastSyncedTransactionId;
			private long _lastSyncedJournal;
			private long _totalWrittenButUnsyncedBytes;
			private DateTime _lastDataFileSyncTime;
			private JournalFile _lastFlushedJournal;

			public JournalApplicator(WriteAheadJournal waj)
			{
				_waj = waj;
			}



			public void ApplyLogsToDataFile(long oldestActiveTransaction, Transaction transaction = null)
			{
				_flushingSemaphore.Wait();

				try
				{
					var alreadyInWriteTx = transaction != null && transaction.Flags == TransactionFlags.ReadWrite;

					var jrnls = _waj._files.Select(x => x.GetSnapshot()).OrderBy(x => x.Number).ToList();
					if (jrnls.Count == 0)
						return; // nothing to do

					Debug.Assert(jrnls.First().Number >= _lastSyncedJournal);

					var pagesToWrite = new Dictionary<long, long>();

					long lastProcessedJournal = -1;
					long previousJournalMaxTransactionId = -1;

					long lastFlushedTransactionId = -1;

					foreach (var journalFile in jrnls.Where(x => x.Number >= _lastSyncedJournal))
					{
						var currentJournalMaxTransactionId = -1L;

						foreach (var pagePosition in journalFile.PageTranslationTable.IterateLatestAsOf(journalFile.LastTransaction))
						{
							if (oldestActiveTransaction != 0 &&
								pagePosition.Value.TransactionId >= oldestActiveTransaction)
							{
								// we cannot write this yet, there is a read transaction that might be looking at this
								// however, we _aren't_ going to be writing this to the data file, since that would be a 
								// waste, we would just overwrite that value in the next flush anyway
								pagesToWrite.Remove(pagePosition.Key);
								continue;
							}

							if (journalFile.Number == _lastSyncedJournal && pagePosition.Value.TransactionId <= _lastSyncedTransactionId)
								continue;

							currentJournalMaxTransactionId = Math.Max(currentJournalMaxTransactionId, pagePosition.Value.TransactionId);

							if (currentJournalMaxTransactionId < previousJournalMaxTransactionId)
								throw new InvalidOperationException(
									"Journal applicator read beyond the oldest active transaction in the next journal file. " +
									"This should never happen. Current journal max tx id: " + currentJournalMaxTransactionId +
									", previous journal max ix id: " + previousJournalMaxTransactionId +
									", oldest active transaction: " + oldestActiveTransaction);


							lastProcessedJournal = journalFile.Number;
							pagesToWrite[pagePosition.Key] = pagePosition.Value.ScratchPos;

							lastFlushedTransactionId = currentJournalMaxTransactionId;
						}

						if (currentJournalMaxTransactionId == -1L)
							continue;

						previousJournalMaxTransactionId = currentJournalMaxTransactionId;
					}

					if (pagesToWrite.Count == 0)
						return;

					_lastSyncedJournal = lastProcessedJournal;
					_lastSyncedTransactionId = lastFlushedTransactionId;

				    try
				    {
				        ApplyPagesToDataFileFromScratch(pagesToWrite, transaction, alreadyInWriteTx);
				    }
				    catch (TimeoutException)
				    {
				        return; // nothing to do, will try again next time
				    }
					catch (DiskFullException diskFullEx)
					{
						_waj._env.HandleDataDiskFullException(diskFullEx);
						return;
					}

					var unusedJournals = GetUnusedJournalFiles(jrnls);

					foreach (var unused in unusedJournals.Where(unused => !_journalsToDelete.ContainsKey(unused.Number)))
					{
						_journalsToDelete.Add(unused.Number, unused);
					}

					using (var txw = alreadyInWriteTx ? null : _waj._env.NewTransaction(TransactionFlags.ReadWrite))
					{
						_lastFlushedJournal = _waj._files.First(x => x.Number == _lastSyncedJournal);

						if (unusedJournals.Count > 0)
						{
							var lastUnusedJournalNumber = unusedJournals.Last().Number;
							_waj._files = _waj._files.RemoveWhile(x => x.Number <= lastUnusedJournalNumber, new List<JournalFile>());
						}

						if (_waj._files.Count == 0)
							_waj.CurrentFile = null;
						
						FreeScratchPages(unusedJournals);

						if (_totalWrittenButUnsyncedBytes > DelayedDataFileSynchronizationBytesLimit ||
							DateTime.Now - _lastDataFileSyncTime > DelayedDataFileSynchronizationTimeLimit)
						{
							_waj._dataPager.Sync();

							UpdateFileHeaderAfterDataFileSync(_lastFlushedJournal, oldestActiveTransaction);

							Debug.Assert(lastFlushedTransactionId != -1);

							_waj._lastFlushedTransaction = lastFlushedTransactionId;

							foreach (var toDelete in _journalsToDelete.Values)
							{
								if (_waj._env.Options.IncrementalBackupEnabled == false)
									toDelete.DeleteOnClose = true;

								toDelete.Release();
							}

							_journalsToDelete.Clear();
							_totalWrittenButUnsyncedBytes = 0;
							_lastDataFileSyncTime = DateTime.Now;
						}

						if (txw != null)
							txw.Commit();
					}
				}
				finally
				{
					_flushingSemaphore.Release();
				}
			}

			private void ApplyPagesToDataFileFromScratch(Dictionary<long, long> pagesToWrite, Transaction transaction, bool alreadyInWriteTx)
			{
				var scratchBufferPool = _waj._env.ScratchBufferPool;
				var scratchPagerState = scratchBufferPool.PagerState;
				scratchPagerState.AddRef();

				try
				{
					var sortedPages = pagesToWrite.OrderBy(x => x.Key)
													.Select(x => scratchBufferPool.ReadPage(x.Value, scratchPagerState))
													.ToList();

					var last = sortedPages.Last();

					var numberOfPagesInLastPage = last.IsOverflow == false ? 1 :
						_waj._env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize);

					EnsureDataPagerSpacing(transaction, last, numberOfPagesInLastPage, alreadyInWriteTx);

					long written = 0;

					foreach (var page in sortedPages)
					{
						written += _waj._dataPager.Write(page);
					}

					_totalWrittenButUnsyncedBytes += written;
				}
				finally
				{
					scratchPagerState.Release();
				}
			}

			private void EnsureDataPagerSpacing(Transaction transaction, Page last, int numberOfPagesInLastPage,
					bool alreadyInWriteTx)
			{
				if (_waj._dataPager.WillRequireExtension(last.PageNumber, numberOfPagesInLastPage) == false)
					return;

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

			private void FreeScratchPages(IEnumerable<JournalFile> unusedJournalFiles)
			{
				foreach (var jrnl in _waj._files)
				{
					jrnl.FreeScratchPagesOlderThan(_waj._env, _lastSyncedTransactionId);
				}
				foreach (var journalFile in unusedJournalFiles)
				{
					journalFile.FreeScratchPagesOlderThan(_waj._env, long.MaxValue);
				}
			}

			private List<JournalFile> GetUnusedJournalFiles(List<JournalSnapshot> jrnls)
			{
				var unusedJournalFiles = new List<JournalFile>();
				foreach (var j in jrnls)
				{
					if (j.Number > _lastSyncedJournal) // after the last log we synced, nothing to do here
						continue;
					if (j.Number == _lastSyncedJournal) // we are in the last log we synced
					{
						if (j.AvailablePages != 0 || //　if there are more pages to be used here or 
						j.PageTranslationTable.MaxTransactionId() != _lastSyncedTransactionId) // we didn't synchronize whole journal
							continue; // do not mark it as unused
					}
					unusedJournalFiles.Add(_waj._files.First(x => x.Number == j.Number));
				}
				return unusedJournalFiles;
			}

			public void UpdateFileHeaderAfterDataFileSync(JournalFile file, long oldestActiveTransaction)
			{
				var txHeaders = stackalloc TransactionHeader[2];
				var readTxHeader = &txHeaders[0];
				var lastReadTxHeader = txHeaders[1];

				var txPos = 0;
				while (true)
				{
					if (file.ReadTransaction(txPos, readTxHeader) == false)
						break;
					if (readTxHeader->HeaderMarker != Constants.TransactionHeaderMarker)
						break;
					if (readTxHeader->TransactionId + 1 == oldestActiveTransaction)
						break;

					lastReadTxHeader = *readTxHeader;
					
					var compressedPages = (readTxHeader->CompressedSize / AbstractPager.PageSize) + (readTxHeader->CompressedSize % AbstractPager.PageSize == 0 ? 0 : 1);

					txPos += compressedPages + 1;
				}

				Debug.Assert(_lastSyncedJournal != -1);
				Debug.Assert(_lastSyncedTransactionId != -1);

				_waj._headerAccessor.Modify(header =>
					{
						header->TransactionId = lastReadTxHeader.TransactionId;
						header->LastPageNumber = lastReadTxHeader.LastPageNumber;

						header->Journal.LastSyncedJournal = _lastSyncedJournal;
						header->Journal.LastSyncedTransactionId = _lastSyncedTransactionId;

						header->Root = lastReadTxHeader.Root;
						header->FreeSpace = lastReadTxHeader.FreeSpace;

						_waj._updateLogInfo(header);
					});
			}

		    public IDisposable TakeFlushingLock()
		    {
		        _flushingSemaphore.Wait();
		        return new DisposableAction(() => _flushingSemaphore.Release());
		    }
		}

		public void WriteToJournal(Transaction tx, int pageCount)
		{
		    var pages = CompressPages(tx, pageCount, _compressionPager);

		    if (CurrentFile == null || CurrentFile.AvailablePages < pages.Length)
		    {
		        CurrentFile = NextFile(pages.Length);
		    }
		    CurrentFile.Write(tx, pages);
		    if (CurrentFile.AvailablePages == 0)
		    {
		        CurrentFile = null;
		    }

		}


		private byte*[] CompressPages(Transaction tx, int numberOfPages, IVirtualPager compressionPager)
		{
			// numberOfPages include the tx header page, which we don't compress
			var dataPagesCount = numberOfPages - 1;
			var sizeInBytes = dataPagesCount * AbstractPager.PageSize;
			var outputBuffer = LZ4.MaximumOutputLength(sizeInBytes);
			var outputBufferInPages = outputBuffer / AbstractPager.PageSize +
									  (outputBuffer % AbstractPager.PageSize == 0 ? 0 : 1);
			var pagesRequired = (dataPagesCount + outputBufferInPages);

			compressionPager.EnsureContinuous(tx, 0, pagesRequired);
			var tempBuffer = compressionPager.AcquirePagePointer(0);
			var compressionBuffer = compressionPager.AcquirePagePointer(dataPagesCount);

			var write = tempBuffer;
			var txPages = tx.GetTransactionPages();

			for (int index = 1; index < txPages.Count; index++)
			{
				var txPage = txPages[index];
				var scratchPage = tx.Environment.ScratchBufferPool.AcquirePagePointer(txPage.PositionInScratchBuffer);
				var count = txPage.NumberOfPages * AbstractPager.PageSize;
				NativeMethods.memcpy(write, scratchPage, count);
				write += count;
			}

			var len = DoCompression(tempBuffer, compressionBuffer, sizeInBytes, outputBuffer);
			var compressedPages = (len / AbstractPager.PageSize) + (len % AbstractPager.PageSize == 0 ? 0 : 1);

			var pages = new byte*[compressedPages + 1];

			var txHeaderBase = tx.Environment.ScratchBufferPool.AcquirePagePointer(txPages[0].PositionInScratchBuffer);
			var txHeader = (TransactionHeader*)txHeaderBase;

			txHeader->Compressed = true;
			txHeader->CompressedSize = len;
			txHeader->UncompressedSize = sizeInBytes;

			pages[0] = txHeaderBase;
			for (int index = 0; index < compressedPages; index++)
			{
				pages[index + 1] = compressionBuffer + (index * AbstractPager.PageSize);
			}

			txHeader->Crc = Crc.Value(compressionBuffer, 0, compressedPages * AbstractPager.PageSize);

			return pages;
		}


		private int DoCompression(byte* input, byte* output, int inputLength, int outputLength)
		{
			var doCompression = _lz4.Encode64(
				input,
				output,
				inputLength,
				outputLength);

			return doCompression;
		}
	}
}
