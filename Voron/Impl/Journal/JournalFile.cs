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
using System.Runtime.InteropServices;
using System.Security;
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
        private readonly List<PagePosition> _unusedPages = new List<PagePosition>();
		private readonly LZ4 _lz4 = new LZ4();
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
                LastTransaction = lastTxId
            };
        }

        public void DisposeWithoutClosingPager()
        {
            if (_disposed)
                return;

            GC.SuppressFinalize(this);

            _disposed = true;
			_lz4.Dispose();
        }

        public bool ReadTransaction(long pos, TransactionHeader* txHeader)
        {
            return _journalWriter.Read(pos, (byte*)txHeader, sizeof(TransactionHeader));
        }

        public Task Write(Transaction tx, int numberOfPages, IVirtualPager compressionPager)
        {
            var txPages = tx.GetTransactionPages();

            var ptt = new Dictionary<long, PagePosition>();
            var unused = new List<PagePosition>();
            var writePagePos = _writePage;


			var pages = CompressPages(tx, numberOfPages, compressionPager, txPages);
			//var pages = UncompressedPages(tx, numberOfPages, txPages);


            UpdatePageTranslationTable(tx, txPages, unused, ptt);

            lock (_locker)
            {
                _writePage += pages.Length;
                _pageTranslationTable.SetItems(tx, ptt);
                _unusedPages.AddRange(unused);
            }

            return _journalWriter.WriteGatherAsync(writePagePos * AbstractPager.PageSize, pages);
        }

		private byte*[] UncompressedPages(Transaction tx, int numberOfPages, List<PageFromScratchBuffer> txPages)
	    {
			var txHeaderBase = tx.Environment.ScratchBufferPool.ReadPage(txPages[0].PositionInScratchBuffer).Base;
			var txHeader = (TransactionHeader*)txHeaderBase;

			txHeader->Compressed = false;
			txHeader->CompressedSize = -1;
			txHeader->UncompressedSize = -1;

			var pages = new byte*[numberOfPages];
			pages[0] = txHeaderBase;
			var pagePos = 1;
			for (int index = 1; index < txPages.Count; index++)
			{
				var txPage = txPages[index];
				for (int i = 0; i < txPage.NumberOfPages; i++)
				{
					var scratchPage = tx.Environment.ScratchBufferPool.AcquirePagePointer(txPage.PositionInScratchBuffer + i);
					pages[pagePos++] = scratchPage;

				}
			}

			return pages;
	    }

	    private byte*[] CompressPages(Transaction tx, int numberOfPages, IVirtualPager compressionPager, List<PageFromScratchBuffer> txPages)
        {
            // numberOfPages include the tx header page, which we don't compress
            var dataPagesCount = numberOfPages - 1;
            var sizeInBytes = dataPagesCount*AbstractPager.PageSize;
            var outputBuffer = LZ4.MaximumOutputLength(sizeInBytes);
            var outputBufferInPages = outputBuffer/AbstractPager.PageSize +
                                      (outputBuffer%AbstractPager.PageSize == 0 ? 0 : 1);
            var pagesRequired = (dataPagesCount + outputBufferInPages);

            compressionPager.EnsureContinuous(tx, 0, pagesRequired);
            var tempBuffer = compressionPager.AcquirePagePointer(0);
            var compressionBuffer = compressionPager.AcquirePagePointer(dataPagesCount);

            var write = tempBuffer;

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

            return pages;
        }

	    private unsafe void UpdatePageTranslationTable(Transaction tx, List<PageFromScratchBuffer> txPages, List<PagePosition> unused, Dictionary<long, PagePosition> ptt)
	    {
		    for (int index = 1; index < txPages.Count; index++)
		    {
			    var txPage = txPages[index];
			    var scratchPage = tx.Environment.ScratchBufferPool.ReadPage(txPage.PositionInScratchBuffer);
			    var pageNumber = ((PageHeader*)scratchPage.Base)->PageNumber;
			    PagePosition value;
			    if (_pageTranslationTable.TryGetValue(tx, pageNumber, out value))
			    {
				    unused.Add(value);
			    }

			    ptt[pageNumber] = new PagePosition
			    {
				    ScratchPos = txPage.PositionInScratchBuffer,
				    JournalPos = -1, // needed only during recovery and calculated there
				    TransactionId = tx.Id
			    };
		    }
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

        public void InitFrom(JournalReader journalReader, Dictionary<long, PagePosition> pageTranslationTable)
        {
            _writePage = journalReader.NextWritePage;
            _pageTranslationTable.SetItemsNoTransaction(pageTranslationTable);
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