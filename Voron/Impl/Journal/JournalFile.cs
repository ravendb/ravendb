// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
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
		private SafeDictionary<long, PagePosition> _pageTranslationTable = SafeDictionary<long, PagePosition>.Empty;
		private SafeDictionary<long, long> _transactionEndPositions = SafeDictionary<long, long>.Empty;
		private SafeList<PagePosition> _unusedPages = SafeList<PagePosition>.Empty;

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

        public SafeDictionary<long, PagePosition> PageTranslationTable
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
            return new JournalSnapshot
            {
                Number = Number,
                AvailablePages = AvailablePages,
                PageTranslationTable = _pageTranslationTable,
                TransactionEndPositions = _transactionEndPositions
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
					if (_pageTranslationTable.TryGetValue(pageNumber, out value))
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
                _writePage += numberOfPages;
                _transactionEndPositions = _transactionEndPositions.Add(tx.Id, lastPagePosition);
                _pageTranslationTable = _pageTranslationTable.SetItems(ptt);
                _unusedPages = _unusedPages.AddRange(unused);
            }

            return _journalWriter.WriteGatherAsync(writePagePos * AbstractPager.PageSize, pages);
        }

        public void InitFrom(JournalReader journalReader, SafeDictionary<long, PagePosition> pageTranslationTable, SafeDictionary<long, long> transactionEndPositions)
        {
            _writePage = journalReader.NextWritePage;
            _pageTranslationTable = pageTranslationTable;
            _transactionEndPositions = transactionEndPositions;
        }

        public bool DeleteOnClose { set { _journalWriter.DeleteOnClose = value; } }

        public void FreeScratchPagesOlderThan(StorageEnvironment env, long oldestActiveTransaction)
        {
            List<KeyValuePair<long, PagePosition>> unusedPages;

	        List<PagePosition> unusedAndFree;
            lock (_locker)
            {
                _unusedPages = _unusedPages.RemoveAllAndGetDiscards(position => position.TransactionId < oldestActiveTransaction, out unusedAndFree);

                unusedPages = _pageTranslationTable.Where(x => x.Value.TransactionId < oldestActiveTransaction).ToList();
                _pageTranslationTable = _pageTranslationTable.RemoveRange(unusedPages.Select(x => x.Key));
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