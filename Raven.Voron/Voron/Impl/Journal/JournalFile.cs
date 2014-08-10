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
        private readonly object _locker = new object();

        public class PagePosition
        {
            protected bool Equals(PagePosition other)
            {
                return ScratchPos == other.ScratchPos && JournalPos == other.JournalPos && TransactionId == other.TransactionId && JournalNumber == other.JournalNumber;
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    int hashCode = ScratchPos.GetHashCode();
                    hashCode = (hashCode * 397) ^ JournalPos.GetHashCode();
                    hashCode = (hashCode * 397) ^ TransactionId.GetHashCode();
                    hashCode = (hashCode * 397) ^ JournalNumber.GetHashCode();
                    return hashCode;
                }
            }

            public long ScratchPos;
            public long JournalPos;
            public long TransactionId;
	        public long JournalNumber;
	        public bool OverwrittenByOverflowPage;

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;
                if (ReferenceEquals(this, obj))
                    return true;
                if (obj.GetType() != GetType())
                    return false;

                return Equals((PagePosition)obj);
            }

	        public override string ToString()
	        {
		        return string.Format("ScratchPos: {0}, JournalPos: {1}, TransactionId: {2}, JournalNumber: {3}", ScratchPos, JournalPos, TransactionId, JournalNumber);
	        }
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
                "Disposing a journal file from finalizer! It should be disposed by using JournalFile.Release() instead!. Log file number: " +
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
            var lastTxId = _pageTranslationTable.GetLastSeenTransactionId();
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
        }

        public bool ReadTransaction(long pos, TransactionHeader* txHeader)
        {
            return _journalWriter.Read(pos, (byte*)txHeader, sizeof(TransactionHeader));
        }

		/// <summary>
		/// write transaction's raw page data into journal. returns write page position
		/// </summary>
		public long Write(Transaction tx, byte*[] pages)
		{
			var txPages = tx.GetTransactionPages();

			var ptt = new Dictionary<long, PagePosition>();
			var unused = new HashSet<PagePosition>();
			var pageWritePos = _writePage;

			UpdatePageTranslationTable(tx, txPages, unused, ptt);

			lock (_locker)
			{
				_writePage += pages.Length;
				_pageTranslationTable.SetItems(tx, ptt);

				Debug.Assert(!_unusedPages.Any(unused.Contains));
				_unusedPages.AddRange(unused);
			}

			var position = pageWritePos * AbstractPager.PageSize;
			_journalWriter.WriteGather(position, pages);

			return pageWritePos;
		}      

	    private void UpdatePageTranslationTable(Transaction tx, List<PageFromScratchBuffer> txPages, HashSet<PagePosition> unused, Dictionary<long, PagePosition> ptt)
	    {
		    for (int index = 1; index < txPages.Count; index++)
		    {
			    var txPage = txPages[index];
			    var scratchPage = tx.Environment.ScratchBufferPool.ReadPage(txPage.PositionInScratchBuffer);
			    var pageNumber = scratchPage.PageNumber;

				for (int i = 0; i < txPage.NumberOfPages; i++)
				{
					PagePosition value;
					if (_pageTranslationTable.TryGetValue(tx, pageNumber + i, out value))
					{
						if(i == 0)
							unused.Add(value);
						else if (i > 0)
						{
							value.OverwrittenByOverflowPage = true;

							// unused.Add(value) - intentionally not adding it here, it will be released in FreeScratchPagesOlderThan 
						}
					}

					PagePosition pagePosition;
					if (ptt.TryGetValue(pageNumber + i, out pagePosition))
					{
						unused.Add(pagePosition);

						if (i > 0)
							ptt.Remove(pageNumber + i); // page currently taken by overflow, need to delete to make sure it won't be copied to the data file
					}
				}

				ptt[pageNumber] = new PagePosition
				{
					ScratchPos = txPage.PositionInScratchBuffer,
					JournalPos = -1, // needed only during recovery and calculated there
					TransactionId = tx.Id,
					JournalNumber = Number
				};
			}
	    }

        public void InitFrom(JournalReader journalReader)
        {
            _writePage = journalReader.NextWritePage;
        }

        public bool DeleteOnClose { set { _journalWriter.DeleteOnClose = value; } }
	    
	    public void FreeScratchPagesOlderThan(Transaction tx, long lastSyncedTransactionId)
        {
            List<KeyValuePair<long, PagePosition>> unusedPages;

            List<PagePosition> unusedAndFree;
            lock (_locker)
            {
                unusedAndFree = _unusedPages.FindAll(position => position.TransactionId < lastSyncedTransactionId);
                _unusedPages.RemoveAll(position => position.TransactionId < lastSyncedTransactionId);

                unusedPages = _pageTranslationTable.AllPagesOlderThan(lastSyncedTransactionId);
                _pageTranslationTable.Remove(unusedPages.Select(x => x.Key), lastSyncedTransactionId);
            }

            foreach (var unusedScratchPage in unusedAndFree)
            {
                tx.Environment.ScratchBufferPool.Free(unusedScratchPage.ScratchPos, tx.Id);
            }

            foreach (var unusedScratchPage in unusedPages)
            {
				tx.Environment.ScratchBufferPool.Free(unusedScratchPage.Value.ScratchPos, tx.Id);
            }
        }
    }
}
