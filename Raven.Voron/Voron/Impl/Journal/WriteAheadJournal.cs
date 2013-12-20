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
	public unsafe class WriteAheadJournal : IDisposable
	{
		private readonly StorageEnvironment _env;
		private readonly IVirtualPager _dataPager;

		private long _currentJournalFileSize;
		private DateTime _lastFile;

		private long _journalIndex = -1;
        private readonly LZ4 _lz4 = new LZ4();
		private readonly SemaphoreSlim _writeSemaphore = new SemaphoreSlim(1, 1);

		private readonly SemaphoreSlim _flushingSemaphore = new SemaphoreSlim(1, 1);
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

			_compressionPager = _env.Options.CreateScratchPager("compression.buffers");
		}

		public ImmutableAppendOnlyList<JournalFile> Files { get { return _files; } }

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

			var lastSyncedTransactionId = logInfo.LastSyncedTransactionId;

			var journalFiles = new List<JournalFile>();
		    long journalNumber;
			for (journalNumber = oldestLogFileStillInUse; journalNumber <= logInfo.CurrentJournal; journalNumber++)
			{
				using (var recoveryPager = _env.Options.CreateScratchPager(StorageEnvironmentOptions.JournalRecoveryName(journalNumber)))
				using (var pager = _env.Options.OpenJournalPager(journalNumber))
				{
					RecoverCurrentJournalSize(pager);


					var transactionHeader = txHeader->TransactionId == 0 ? null : txHeader;
					var journalReader = new JournalReader(pager, recoveryPager, lastSyncedTransactionId, transactionHeader);
					journalReader.RecoverAndValidate(_env.Options);

					// after reading all the pages from the journal file, we need to move them to the scratch buffers.
					var ptt = new Dictionary<long, JournalFile.PagePosition>();
					foreach (var kvp in journalReader.TransactionPageTranslation)
					{
						var page = recoveryPager.Read(kvp.Value.JournalPos);
						var numOfPages = page.IsOverflow ? recoveryPager.GetNumberOfOverflowPages(page.OverflowSize) : 1;
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
					jrnlFile.InitFrom(journalReader, ptt);
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
			_files = _files.AppendRange(journalFiles);

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

			_writeSemaphore.Wait();

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

		public void Clear(Transaction tx)
		{
			if(tx.Flags != TransactionFlags.ReadWrite)
				throw new InvalidOperationException("Clearing of write ahead journal should be called only from a write transaction");

			foreach (var journalFile in _files)
			{
				journalFile.Release();
			}
			_files = ImmutableAppendOnlyList<JournalFile>.Empty;
			CurrentFile = null;
		}

		public class JournalApplicator : IDisposable
		{
			private readonly WriteAheadJournal _waj;
			private readonly long _oldestActiveTransaction;
			private long _lastSyncedTransactionId;
			private long _lastSyncedJournal;
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
				_lastSyncedTransactionId = journalInfo.LastSyncedTransactionId;
                Debug.Assert(_jrnls.First().Number >= _lastSyncedJournal);

				var pagesToWrite = new Dictionary<long, long>();

				long lastProcessedJournal = -1;
				long previousJournalMaxTransactionId = -1;

				long lastFlushedTransactionId = -1;

			    foreach (var journalFile in _jrnls.Where(x => x.Number >= _lastSyncedJournal))
                {
	                var currentJournalMaxTransactionId = -1L;

                    foreach (var pagePosition in journalFile.PageTranslationTable.IterateLatestAsOf(journalFile.LastTransaction))
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

						if(journalFile.Number == _lastSyncedJournal && pagePosition.Value.TransactionId <= _lastSyncedTransactionId)
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

	                if (currentJournalMaxTransactionId == -1L)
		                continue;

	                previousJournalMaxTransactionId = currentJournalMaxTransactionId;
                }

                if (pagesToWrite.Count == 0)
                    return;

				_lastSyncedJournal = lastProcessedJournal;
				_lastSyncedTransactionId = lastFlushedTransactionId;

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

					//DebugValidateWrittenTransaction();

					EnsureDataPagerSpacing(transaction, last, numberOfPagesInLastPage, alreadyInWriteTx);

			        foreach (var page in sortedPages)
			        {
			            _waj._dataPager.Write(page);
			        }

					_waj._dataPager.Sync();
				}
				catch (DiskFullException diskFullEx)
				{
					_waj._env.HandleDataDiskFullException(diskFullEx);
					return;
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
						_waj._files = _waj._files.RemoveWhile(x => x.Number <= lastJournalFileToRemove.Number, new List<JournalFile>());

                    _waj.UpdateLogInfo();

					if (_waj._files.Count == 0)
                    {
                        _waj.CurrentFile = null;
                    }

					Debug.Assert(lastFlushedTransactionId != -1);

	                _waj._lastFlushedTransaction = lastFlushedTransactionId;

					FreeScratchPages(txw ?? transaction, unusedJournalFiles);

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

			//[Conditional("DEBUG")]
			//private void DebugValidateWrittenTransaction()
			//{
			//	var txHeaders = stackalloc TransactionHeader[1];
			//	var readTxHeader = &txHeaders[0];
			//	_waj._writeSemaphore.Wait();
			//	try
			//	{
			//		var jrnl = _waj._files.First(x => x.Number == _lastSyncedJournal);
			//		var read = jrnl.ReadTransaction(_lastSyncedPage + 1, readTxHeader);

			//		if (readTxHeader->HeaderMarker != 0 && (read == false || readTxHeader->HeaderMarker != Constants.TransactionHeaderMarker || readTxHeader->TransactionId != _lastSyncedTransactionId + 1))
			//		{
			//			var start = 0;
			//			var positions = new Dictionary<long, TransactionHeader>();
			//			while (jrnl.ReadTransaction(start, readTxHeader))
			//			{
			//				positions.Add(start, *readTxHeader);
			//				start += readTxHeader->PageCount + readTxHeader->OverflowPageCount + 1;
			//			}

			//			throw new InvalidOperationException(
			//				"Reading transaction for calculated page in journal failed. "
			//				+ "Journal #" + _lastSyncedJournal + ". "
			//				+ "Page #" + (_lastSyncedPage + 1) + ". "
			//				+ "This means that page calculation is wrong and should be fixed otherwise data will be lost during database recovery from this journal.");
			//		}
			//	}
			//	finally
			//	{
			//		_waj._writeSemaphore.Release();
			//	}
			//}

			private void FreeScratchPages(Transaction tx, IEnumerable<JournalFile> unusedJournalFiles)
			{
				foreach (var jrnl in _waj._files)
				{
					jrnl.FreeScratchPagesOlderThan(tx, _waj._env, _oldestActiveTransaction);
				}
				foreach (var journalFile in unusedJournalFiles)
				{
					if (_waj._env.Options.IncrementalBackupEnabled == false)
						journalFile.DeleteOnClose = true;
					journalFile.FreeScratchPagesOlderThan(tx, _waj._env, long.MaxValue);
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
                        j.PageTranslationTable.MaxTransactionId() != _lastSyncedTransactionId) // we didn't synchronize whole journal
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
				Debug.Assert(_lastSyncedTransactionId != -1);

				_waj._headerAccessor.Modify(header =>
					{
						header->TransactionId = lastReadTxHeader.TransactionId;
						header->LastPageNumber = lastReadTxHeader.LastPageNumber;

						header->Journal.LastSyncedJournal = _lastSyncedJournal;
						header->Journal.LastSyncedTransactionId = _lastSyncedTransactionId;

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
			    var pages = CompressPages(tx, pageCount, _compressionPager);

                if (CurrentFile == null || CurrentFile.AvailablePages < pages.Length)
				{
					CurrentFile = NextFile(pages.Length);
				}
				var task = CurrentFile.Write(tx, pages)
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

#if DEBUG
            //var mem = Marshal.AllocHGlobal(inputLength);
            //try
            //{
            //	var len = LZ4.Decode64(output, doCompression, (byte*) mem.ToPointer(),
            //		inputLength, true);

            //	var result = NativeMethods.memcmp(input, (byte*) mem.ToPointer(),
            //		inputLength);

            //	Debug.Assert(len == inputLength);
            //	Debug.Assert(result == 0);

            //}
            //finally
            //{
            //	Marshal.FreeHGlobal(mem);
            //}
#endif

            return doCompression;
        }
	}
}
