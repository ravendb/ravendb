// -----------------------------------------------------------------------
//  <copyright file="LogFile.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Immutable;
using System.Diagnostics;
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

        private readonly ReaderWriterLockSlim _locker = new ReaderWriterLockSlim();

        public class PagePosition
        {
            public long ScratchPos;
            public long JournalPos;
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

        private int _refs;
        private ImmutableDictionary<long, PagePosition> _pageTranslationTable;

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
            if (DeleteOnClose)
            {
                Console.WriteLine("TODO: Delete me");
            }
        }

        public Reader GetReader()
        {
            _locker.EnterReadLock();
            try
            {
                return new Reader(_journalWriter.CreatePager(), _writePage);
            }
            finally
            {
                _locker.ExitReadLock();
            }
        }

        public JournalSnapshot GetSnapshot()
        {
            _locker.EnterReadLock();
            try
            {
                return new JournalSnapshot
                {
                    Number = Number,
                    PageTranslationTable = _pageTranslationTable
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
                        ptt = ptt.SetItem(((PageHeader*)txPage.Pointer)->PageNumber, new PagePosition
                        {
                            ScratchPos = txPage.PositionInScratchBuffer,
                            JournalPos = writePagePos + index
                        });
                        for (int i = 0; i < txPage.NumberOfPages; i++)
                        {
                            pages[pagesCounter++] = txPage.Pointer + (i * AbstractPager.PageSize);
                        }
                    }
                }
                Debug.Assert(pagesCounter == numberOfPages);


                _writePage += numberOfPages;
                _pageTranslationTable = ptt;
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

        public class Reader : IDisposable
        {
            private readonly IVirtualPager _pager;
            private readonly long _lastWritePos;
            private ImmutableDictionary<long, long> _pageTranslationTable = ImmutableDictionary<long, long>.Empty;

            public bool RequireHeaderUpdate { get; private set; }

            public Reader(IVirtualPager pager, long lastWritePos)
            {
                _pager = pager;
                _lastWritePos = lastWritePos;
            }

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

            public long LastWritePos
            {
                get { return _lastWritePos; }
            }

            public IVirtualPager Pager { get { return _pager; } }

            public TransactionHeader* LastTransactionHeader;

            public void Dispose()
            {
                _pager.Dispose();
            }
        }

        public bool DeleteOnClose { get; set; }
    }
}