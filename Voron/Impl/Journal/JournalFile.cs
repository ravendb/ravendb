// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Voron.Impl.Paging;
using Voron.Trees;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class JournalFile : IDisposable
    {
        private readonly IJournalWriter _journalWriter;
        private long _writePage;
        private bool _disposed;
        private int _refs;
	    private readonly PageTable _pageTranslationTable = new PageTable();
		private ImmutableAppendOnlyList<KeyValuePair<long, long>> _transactionEndPositions = ImmutableAppendOnlyList<KeyValuePair<long, long>>.Empty;
		private readonly List<PagePosition> _unusedPages = new List<PagePosition>();

        private readonly object _locker = new object();

        public class PagePosition
        {
            public long ScratchPos;
            public long JournalPos;
            public long TransactionId;
        }

        public JournalFile(IJournalWriter journalWriter, long journalNumber)
        {
            Number = journalNumber;
            _journalWriter = journalWriter;
            _writePage = 0;
        }

        public override string ToString()
        {
            return string.Format("Number: {0}", Number);
        }

        public JournalFile(IJournalWriter journalWriter, long journalNumber, long lastSyncedPage)
            : this(journalWriter, journalNumber)
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


        public long AvailablePages
        {
            get { return _journalWriter.NumberOfAllocatedPages - _writePage; }
        }

	    internal IJournalWriter JournalWriter
	    {
			get { return _journalWriter; }
	    }

		public PageTable PageTranslationTable
        {
            get { return _pageTranslationTable; }
        }

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
            _journalWriter.Dispose();
        }

        public JournalSnapshot GetSnapshot()
        {
	        var lastTxId = _pageTranslationTable.GetLastSeenTransaction();
            return new JournalSnapshot
            {
                Number = Number,
                AvailablePages = AvailablePages,
                PageTranslationTable = _pageTranslationTable,
                TransactionEndPositions = _transactionEndPositions,
				LastTransaction = lastTxId
            };
        }

        public void DisposeWithoutClosingPager()
        {
            if (_disposed)
                return;

            GC.SuppressFinalize(this);

            _disposed = true;
        }

        public bool ReadTransaction(long pos, TransactionHeader* txHeader)
        {
            return _journalWriter.Read(pos, (byte*) txHeader, sizeof (TransactionHeader));
        }

        public Task Write(Transaction tx, int numberOfPages)
        {
            var pages = new byte*[numberOfPages];
            var pagesCounter = 0;
            var txPages = tx.GetTransactionPages();

            var journalPos = -1L;
            var ptt = new Dictionary<long, PagePosition>();
			var unused = new List<PagePosition>();
			var writePagePos = _writePage;

            PageFromScratchBuffer previousPage = null;
            var numberOfOverflows = 0;

			for (int index = 0; index < txPages.Count; index++)
			{
				var txPage = txPages[index];
				var scratchPage = tx.Environment.ScratchBufferPool.ReadPage(txPage.PositionInScratchBuffer);
				if (index == 0) // this is the transaction header page
				{
					pages[pagesCounter++] = scratchPage.Base;
				}
				else
				{
					var pageNumber = ((PageHeader*)scratchPage.Base)->PageNumber;
					PagePosition value;
					if (_pageTranslationTable.TryGetValue(tx, pageNumber, out value))
					{
						unused.Add(value);
					}

					numberOfOverflows += previousPage != null ? previousPage.NumberOfPages - 1 : 0;
					journalPos = writePagePos + index + numberOfOverflows;

					ptt[pageNumber] = new PagePosition
					{
						ScratchPos = txPage.PositionInScratchBuffer,
                        JournalPos = journalPos,
						TransactionId = tx.Id
					};

					for (int i = 0; i < txPage.NumberOfPages; i++)
					{
						pages[pagesCounter++] = scratchPage.Base + (i * AbstractPager.PageSize);
					}

					previousPage = txPage;
				}
			}

			Debug.Assert(pagesCounter == numberOfPages);
            Debug.Assert(journalPos != -1);

            var lastPagePosition = journalPos 
                + (previousPage != null ? previousPage.NumberOfPages - 1 : 0); // for overflows

            lock (_locker)
            {
				_pageTranslationTable.SetItems(tx, ptt);
				_writePage += numberOfPages;
				_transactionEndPositions = _transactionEndPositions.Append(new KeyValuePair<long, long>(tx.Id, lastPagePosition));
                _unusedPages.AddRange(unused);
            }

            return _journalWriter.WriteGatherAsync(writePagePos * AbstractPager.PageSize, pages);
        }

		public void InitFrom(JournalReader journalReader, Dictionary<long, PagePosition> pageTranslationTable,
			ImmutableAppendOnlyList<KeyValuePair<long, long>> transactionEndPositions)
        {
            _writePage = journalReader.NextWritePage;
			_pageTranslationTable.SetItemsNoTransaction(pageTranslationTable);
            _transactionEndPositions = transactionEndPositions;
        }

        public bool DeleteOnClose { set { _journalWriter.DeleteOnClose = value; } }

        public void FreeScratchPagesOlderThan(Transaction tx, StorageEnvironment env, long oldestActiveTransaction)
        {
            List<KeyValuePair<long, PagePosition>> unusedPages;

	        List<PagePosition> unusedAndFree;
            lock (_locker)
            {
	            unusedAndFree = _unusedPages.FindAll(position => position.TransactionId < oldestActiveTransaction);
                _unusedPages.RemoveAll(position => position.TransactionId < oldestActiveTransaction);

                unusedPages = _pageTranslationTable.AllPagesOlderThan(oldestActiveTransaction);
                _pageTranslationTable.Remove(tx, unusedPages.Select(x => x.Key));
            }

            foreach (var unusedScratchPage in unusedAndFree)
            {
                env.ScratchBufferPool.Free(unusedScratchPage.ScratchPos);
            }

            foreach (var unusedScratchPage in unusedPages)
            {
                env.ScratchBufferPool.Free(unusedScratchPage.Value.ScratchPos);
            }
        }
    }
}