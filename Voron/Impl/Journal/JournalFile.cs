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
using Lz4Net;
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
		private LinkedDictionary<long, PagePosition> _pageTranslationTable = LinkedDictionary<long, PagePosition>.Empty;
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

		public LinkedDictionary<long, PagePosition> PageTranslationTable
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
				PageTranslationTable = _pageTranslationTable
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
			return _journalWriter.Read(pos, (byte*)txHeader, sizeof(TransactionHeader));
		}

		public Task Write(Transaction tx, int numberOfPages, IVirtualPager compressionPager)
        {
            var txPages = tx.GetTransactionPages();

            var ptt = new Dictionary<long, PagePosition>();
			var unused = new List<PagePosition>();
			var writePagePos = _writePage;


	        var pages = CompressPages(tx, numberOfPages, compressionPager, txPages);

			UpdatePageTranslationTable(tx, txPages, unused, ptt);

            lock (_locker)
            {
                _writePage += pages.Length;
                _pageTranslationTable = _pageTranslationTable.SetItems(ptt);
                _unusedPages = _unusedPages.AddRange(unused);
            }

            return _journalWriter.WriteGatherAsync(writePagePos * AbstractPager.PageSize, pages);
        }

		private unsafe void UpdatePageTranslationTable(Transaction tx, List<PageFromScratchBuffer> txPages, List<PagePosition> unused, Dictionary<long, PagePosition> ptt)
		{
			for (int index = 1; index < txPages.Count; index++)
			{
				var txPage = txPages[index];
				var scratchPage = tx.Environment.ScratchBufferPool.ReadPage(txPage.PositionInScratchBuffer);
				var pageNumber = ((PageHeader*) scratchPage.Base)->PageNumber;
				PagePosition value;
				if (_pageTranslationTable.TryGetValue(pageNumber, out value))
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

		private static byte*[] CompressPages(Transaction tx, int numberOfPages, IVirtualPager compressionPager, List<PageFromScratchBuffer> txPages)
		{
			compressionPager.EnsureContinuous(tx, 0, (numberOfPages * 2) + 1);
			var tempBuffer = compressionPager.GetWritable(0);
			NativeMethods.memset(tempBuffer.Base, 0, ((numberOfPages*2) + 1)*AbstractPager.PageSize);

			var write = tempBuffer.Base;

			for (int index = 1; index < txPages.Count; index++)
			{
				var txPage = txPages[index];
				var scratchPage = tx.Environment.ScratchBufferPool.ReadPage(txPage.PositionInScratchBuffer);
				var count = txPage.NumberOfPages*AbstractPager.PageSize;
				NativeMethods.memcpy(write, scratchPage.Base, count);
				write += count;
			}

			var compressionBuffer = compressionPager.GetWritable(numberOfPages - 1);

			var len = DoCompression(numberOfPages, tempBuffer, compressionBuffer);
			var compressedPages = (len/AbstractPager.PageSize) + (len%AbstractPager.PageSize == 0 ? 0 : 1);

			var pages = new byte*[compressedPages + 1];

			var txHeaderBase = tx.Environment.ScratchBufferPool.ReadPage(txPages[0].PositionInScratchBuffer).Base;
			var txHeader = (TransactionHeader*)txHeaderBase;

			txHeader->Compressed = true;
			txHeader->CompressedSize = len;
			txHeader->UncompressedSize = numberOfPages * AbstractPager.PageSize;

			pages[0] = txHeaderBase;
			for (int index = 0; index < compressedPages; index++)
			{
				pages[index + 1] = compressionBuffer.Base + (index * AbstractPager.PageSize);
			}

			return pages;
		}

		private static unsafe int DoCompression(int numberOfPages, Page tempBuffer, Page compressionBuffer)
		{
			var len = Lz4.LZ4_compress(tempBuffer.Base, compressionBuffer.Base, numberOfPages*AbstractPager.PageSize);
			return len;
		}

		public void InitFrom(JournalReader journalReader, LinkedDictionary<long, PagePosition> pageTranslationTable)
		{
			_writePage = journalReader.NextWritePage;
			_pageTranslationTable = pageTranslationTable;
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