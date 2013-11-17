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
using System.Threading.Tasks;
using Voron.Trees;

namespace Voron.Impl.Journal
{
    public unsafe class JournalFile : IDisposable
    {
        private readonly IJournalWriter _journalWriter;
        private long _writePage;
        private bool _disposed;
        private int _refs;
        private ImmutableDictionary<long, PagePosition> _pageTranslationTable;
        private ImmutableList<PagePosition> _unusedPages = ImmutableList<PagePosition>.Empty;

        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

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


        public ImmutableDictionary<long, PagePosition> PageTranslationTable
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
            _locker.EnterReadLock();
            try
            {
                return new JournalSnapshot
                {
                    Number = Number,
                    AvailablePages = AvailablePages,
                    PageTranslationTable = _pageTranslationTable,
                };
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }

        public void DisposeWithoutClosingPager()
        {
            if (_disposed)
                return;

            GC.SuppressFinalize(this);

            _disposed = true;
        }

        public void ReadTransaction(long pos, TransactionHeader* txHeader)
        {
            _journalWriter.Read(pos, (byte*) txHeader, sizeof (TransactionHeader));
        }

        public Task Write(Transaction tx, int numberOfPages)
        {
            var pages = new byte*[numberOfPages];
            int pagesCounter = 0;
            var txPages = tx.GetTransactionPages();

            long writePagePos;
            _locker.EnterWriteLock();
            try
            {
                var ptt = _pageTranslationTable;
                var unused = _unusedPages;
                writePagePos = _writePage;

                for (int index = 0; index < txPages.Count; index++)
                {
                    var txPage = txPages[index];
                    if (index == 0) // this is the transaction header page
                    {
                        pages[pagesCounter++] = txPage.Pointer;
                    }
                    else
                    {
                        var pageNumber = ((PageHeader*) txPage.Pointer)->PageNumber;
                        PagePosition value;
                        if (ptt.TryGetValue(pageNumber, out value))
                        {
                            unused = unused.Add(value);
                        }
                        ptt = ptt.SetItem(pageNumber, new PagePosition
                        {
                            ScratchPos = txPage.PositionInScratchBuffer,
                            JournalPos = writePagePos + index,
                            TransactionId = tx.Id
                        });
                        for (int i = 0; i < txPage.NumberOfPages; i++)
                        {
                            pages[pagesCounter++] = txPage.Pointer + (i*AbstractPager.PageSize);
                        }
                    }
                }
                Debug.Assert(pagesCounter == numberOfPages);

                _writePage += numberOfPages;
                _pageTranslationTable = ptt;
                _unusedPages = unused;
            }
            finally
            {
                _locker.ExitWriteLock();
            }

            return _journalWriter.WriteGatherAsync(writePagePos, pages);
        }

        public void InitFrom(JournalReader journalReader, ImmutableDictionary<long, PagePosition> pageTranslationTable)
        {
            _writePage = journalReader.NextWritePage;
            _pageTranslationTable = pageTranslationTable;
        }

        public IEnumerable<long> UnusedScratchPages
        {
            get { return _unusedPages.Select(x => x.ScratchPos); }
        }

        public bool DeleteOnClose;

        public void FreeScratchPagesOlderThan(StorageEnvironment env, long oldestActiveTransaction)
        {
            var unusedAndFree = _unusedPages.FindAll(position => position.TransactionId < oldestActiveTransaction);
            _unusedPages = _unusedPages.RemoveRange(unusedAndFree);
            foreach (var unusedScratchPage in unusedAndFree)
            {
                env.ScratchBufferPool.Free(unusedScratchPage.ScratchPos);
            }
            var unusedPages = _pageTranslationTable.Where(x => x.Value.TransactionId < oldestActiveTransaction).ToList();
            _pageTranslationTable = _pageTranslationTable.RemoveRange(unusedPages.Select(x => x.Key));
            foreach (var unusedScratchPage in unusedPages)
            {
                env.ScratchBufferPool.Free(unusedScratchPage.Value.ScratchPos);
            }
        }
    }
}