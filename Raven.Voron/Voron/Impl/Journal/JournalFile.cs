// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public unsafe class JournalFile : IDisposable
	{
		private readonly IVirtualPager _pager;
		private ImmutableDictionary<long, long> _pageTranslationTable = ImmutableDictionary<long, long>.Empty;
		private long _writePage;
		private bool _disposed;

		public JournalFile(IVirtualPager pager, long logNumber)
		{
			Number = logNumber;
			_pager = pager;
			_writePage = 0;
		}

		public override string ToString()
		{
			return string.Format("Number: {0}", Number);
		}

		public JournalFile(IVirtualPager pager, long logNumber, long lastSyncedPage)
			: this(pager, logNumber)
		{
			_writePage = lastSyncedPage + 1;
		}

#if DEBUG
		private readonly StackTrace _st = new StackTrace(true);
#endif

		~JournalFile()
		{
			Dispose();

#if DEBUG
			Trace.WriteLine(
				"Disposing a journal file from finalizer! It should be diposed by using JournalFile.Release() instead!. Log file number: " +
				Number + ". Number of references: " + _refs + " " + _st);
#endif
		}

		internal long WritePagePosition
		{
			get { return _writePage; }
		}

		public long Number { get; private set; }

		public IEnumerable<long> GetModifiedPages(long? lastLogPageSyncedWithDataFile)
		{
			if (lastLogPageSyncedWithDataFile == null)
				return _pageTranslationTable.Keys;

			return _pageTranslationTable.Where(x => x.Value > lastLogPageSyncedWithDataFile).Select(x => x.Key);
		}
		public TransactionHeader* LastTransactionHeader { get; private set; }

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

		public bool RequireHeaderUpdate { get; private set; }

	    public Page ReadPage(long pageNumber)
		{
			long logPageNumber;

			if (_pageTranslationTable.TryGetValue(pageNumber, out logPageNumber))
				return _pager.Read(logPageNumber);

			return null;
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
			DisposeWithoutClosingPager();
			_pager.Dispose();

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
			RequireHeaderUpdate = logFileReader.RequireHeaderUpdate;
			
			_pageTranslationTable = _pageTranslationTable.SetItems(logFileReader.TransactionPageTranslation);
			_writePage = logFileReader.WritePage;
			LastTransactionHeader = logFileReader.LastTransactionHeader;

			return logFileReader.LastTransactionHeader;
		}

		public TransactionHeader* RecoverAndValidateConditionally(long startRead, TransactionHeader* lastTxHeader,Func<TransactionHeader,bool> stopConditionFunc)
		{
			var logFileReader = new JournalReader(_pager, startRead, lastTxHeader);
			logFileReader.RecoverAndValidateConditionally(stopConditionFunc);
			RequireHeaderUpdate = logFileReader.RequireHeaderUpdate;

			_pageTranslationTable = _pageTranslationTable.SetItems(logFileReader.TransactionPageTranslation);
			_writePage = logFileReader.WritePage;
			LastTransactionHeader = logFileReader.LastTransactionHeader;

			return logFileReader.LastTransactionHeader;
		}


		public void DisposeWithoutClosingPager()
		{
			if (_disposed)
				return;

			GC.SuppressFinalize(this);

			_disposed = true;
		}

	    public void Write(Transaction tx, int numberOfPages)
	    {
	        _pager.WriteDirect(tx.GetFirstScratchPage(), _writePage ,numberOfPages);
	        _pageTranslationTable = _pageTranslationTable.SetItems(
                tx.PageTranslations.Select(kvp => new KeyValuePair<long,long>(kvp.Key, kvp.Value + _writePage))
	            );
	        _writePage += numberOfPages;
	    }
	}
}