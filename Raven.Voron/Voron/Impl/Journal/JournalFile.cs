// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class JournalFile : IDisposable
	{
		private const int PagesTakenByHeader = 1;

		private readonly IVirtualPager _pager;
		private ImmutableDictionary<long, long> _pageTranslationTable = ImmutableDictionary<long, long>.Empty;
		private readonly Dictionary<long, long> _transactionPageTranslationTable = new Dictionary<long, long>();
		private long _writePage;
		private long _lastSyncedPage = -1;
		private int _allocatedPagesInTransaction;
		private int _overflowPagesInTransaction;
		private TransactionHeader* _currentTxHeader = null;
		private bool _disposed;

		public JournalFile(IVirtualPager pager, long logNumber)
		{
			Number = logNumber;
			_pager = pager;
			_writePage = 0;
		}

		public JournalFile(IVirtualPager pager, long logNumber, long lastSyncedPage)
			: this(pager, logNumber)
		{
			_lastSyncedPage = lastSyncedPage;
			_writePage = lastSyncedPage + 1;
		}

		~JournalFile()
		{
			if (_disposed == false)
			{
				Dispose();

				Trace.WriteLine(
					"Disposing a log file from finalizer! It should be diposed by using LogFile.Release() instead!. Log file number: " +
					Number + ". Number of references: " + _refs);
			}
		}

		internal long WritePagePosition
		{
			get { return _writePage; }
		}

		public long Number { get; private set; }

		public IEnumerable<long> GetModifiedPages(long? lastLogPageSyncedWithDataFile)
		{
			if(lastLogPageSyncedWithDataFile == null)
				return _pageTranslationTable.Keys;

			return _pageTranslationTable.Where(x => x.Value > lastLogPageSyncedWithDataFile).Select(x => x.Key);
		}

		public bool LastTransactionCommitted
		{
			get
			{
				if (_currentTxHeader != null)
				{
					Debug.Assert(_currentTxHeader->TxMarker.HasFlag(TransactionMarker.Commit) == false);
					return false;
				}
				return true;
			}
		}

		public void TransactionBegin(Transaction tx)
		{
			if (LastTransactionCommitted == false)
			{
				// last transaction did not commit, we need to move back the write page position
				_writePage = _lastSyncedPage + 1;
			}

			_currentTxHeader = GetTransactionHeader();

			_currentTxHeader->TransactionId = tx.Id;
			_currentTxHeader->NextPageNumber = tx.State.NextPageNumber;
			_currentTxHeader->LastPageNumber = -1;
			_currentTxHeader->PageCount = -1;
			_currentTxHeader->Crc = 0;
			_currentTxHeader->TxMarker = TransactionMarker.Start;

			_allocatedPagesInTransaction = 0;
			_overflowPagesInTransaction = 0;

			_transactionPageTranslationTable.Clear();
		}

		public void TransactionSplit(Transaction tx)
		{
			if (_currentTxHeader != null)
			{
				_currentTxHeader->TxMarker |= TransactionMarker.Split;
			}
			else
			{
				_currentTxHeader = GetTransactionHeader();
				_currentTxHeader->TransactionId = tx.Id;
				_currentTxHeader->NextPageNumber = tx.State.NextPageNumber;
				_currentTxHeader->TxMarker = TransactionMarker.Split;
				_currentTxHeader->PageCount = -1;
				_currentTxHeader->Crc = 0;
			}	
		}

		public void TransactionCommit(Transaction tx)
		{
			_pageTranslationTable = _pageTranslationTable.SetItems(_transactionPageTranslationTable);

			_transactionPageTranslationTable.Clear();

			_currentTxHeader->LastPageNumber = tx.State.NextPageNumber - 1;
			_currentTxHeader->TxMarker |= TransactionMarker.Commit;
			_currentTxHeader->PageCount = _allocatedPagesInTransaction;
			_currentTxHeader->OverflowPageCount = _overflowPagesInTransaction;
			tx.State.Root.State.CopyTo(&_currentTxHeader->Root);
            tx.State.FreeSpaceRoot.State.CopyTo(&_currentTxHeader->FreeSpace);

			var crcOffset = (int) (_currentTxHeader->PageNumberInLogFile + PagesTakenByHeader)*_pager.PageSize;
			var crcCount = (_allocatedPagesInTransaction + _overflowPagesInTransaction)*_pager.PageSize;

			_currentTxHeader->Crc = Crc.Value(_pager.PagerState.Base, crcOffset, crcCount);

			_currentTxHeader = null;

			Sync();
		}


		private TransactionHeader* GetTransactionHeader()
		{
			var result = (TransactionHeader*) Allocate(-1, PagesTakenByHeader).Base;
			result->HeaderMarker = Constants.TransactionHeaderMarker;
			result->PageNumberInLogFile = _writePage - PagesTakenByHeader;

			return result;
		}

		public long AvailablePages
		{
			get { return _pager.NumberOfAllocatedPages - _writePage; }
		}

		internal IVirtualPager Pager
		{
			get { return _pager; }
		}

        public ImmutableDictionary<long, long> PageTranslationTable
        {
            get { return _pageTranslationTable; }
        }

        private void Sync()
		{
			var start = _lastSyncedPage + 1;
			var count = _writePage - start;

			_pager.Flush(start, count);
			_pager.Sync();

			_lastSyncedPage += count;
		}

		public Page ReadPage(Transaction tx, long pageNumber)
		{
			long logPageNumber;

			if (tx != null &&
				_currentTxHeader != null && _currentTxHeader->TransactionId == tx.Id // we are in the log file where we are currently writing in
				&& _transactionPageTranslationTable.TryGetValue(pageNumber, out logPageNumber))
				return _pager.Read(logPageNumber);

			if (_pageTranslationTable.TryGetValue(pageNumber, out logPageNumber))
				return _pager.Read(logPageNumber);
			
			return null;
		}

		public Page Allocate(long startPage, int numberOfPages)
		{
			Debug.Assert(_writePage + numberOfPages <= _pager.NumberOfAllocatedPages);

			var result = _pager.GetWritable(_writePage);

			if (startPage != -1) // internal use - transaction header allocation, so we don't want to count it as allocated by transaction
			{
				// we allocate more than one page only if the page is an overflow
				// so here we don't want to create mapping for them too
				_transactionPageTranslationTable[startPage] = _writePage;

				_allocatedPagesInTransaction++;

				if (numberOfPages > 1)
				{
					_overflowPagesInTransaction += (numberOfPages - 1);
				}
			}

			_writePage += numberOfPages;

			return result;
		}


		public void DeleteOnClose()
		{
			_pager.DeleteOnClose = true;
		}

		private int _refs;

		public void Release()
		{
			if (Interlocked.Decrement(ref _refs) != 0)
				return;

			Dispose();
		}

		public void AddRef()
		{
			Interlocked.Increment(ref _refs);
		}

		public void Dispose()
		{
			if(_disposed)
				throw new ObjectDisposedException("Log file is already disposed");

			_pager.Dispose();

			_disposed = true;
		}

		public LogSnapshot GetSnapshot()
		{
			return new LogSnapshot
				{
					File = this,
					PageTranslations = _pageTranslationTable
				};
		}

        public TransactionHeader* RecoverAndValidate(long startRead, TransactionHeader* lastTxHeader)
        {
            var logFileReader = new JournalReader(_pager, startRead, lastTxHeader);
            logFileReader.RecoverAndValidate();

            _pageTranslationTable = _pageTranslationTable.SetItems(logFileReader.TransactionPageTranslation);
            _writePage = logFileReader.WritePage;
            _lastSyncedPage = logFileReader.LastSyncedPage;

            return logFileReader.LastTransactionHeader;
        }
	}
}