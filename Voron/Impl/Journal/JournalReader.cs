using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public unsafe class JournalReader
	{
		private readonly IVirtualPager _pager;
		private long _lastSyncedPage;
		private long _nextWritePage;
		private long _readingPage;
		private SafeDictionary<long, JournalFile.PagePosition> _transactionPageTranslation = SafeDictionary<long, JournalFile.PagePosition>.Empty;
		private SafeDictionary<long, long> _transactionEndPositions = SafeDictionary<long, long>.Empty;

		public bool RequireHeaderUpdate { get; private set; }

		public bool EncounteredStopCondition { get; private set; }

		public long NextWritePage
		{
			get { return _nextWritePage; }
		}

		public long LastSyncedPage
		{
			get { return _lastSyncedPage; }
		}

		public JournalReader(IVirtualPager pager, long startPage, TransactionHeader* previous)
		{
			RequireHeaderUpdate = false;
			_pager = pager;
			_readingPage = startPage;
			_nextWritePage = startPage;
			LastTransactionHeader = previous;

		}

		public TransactionHeader* LastTransactionHeader { get; private set; }

		public delegate bool FilterTransactions(long txId, long pageNumber);

		public bool ReadOneTransaction(FilterTransactions stopReadingCondition = null, bool checkCrc = true)
		{
			if (_readingPage >= _pager.NumberOfAllocatedPages)
				return false;

			var transactionTable = new Dictionary<long, JournalFile.PagePosition>();

			TransactionHeader* current;
			if (!TryReadAndValidateHeader(out current)) return false;

			if (stopReadingCondition != null && !stopReadingCondition(current->TransactionId, _readingPage))
			{
				_readingPage--; // if the read tx header does not fulfill our condition we have to move back the read index to allow read it again later if needed
				EncounteredStopCondition = true;
				return false;
			}

			uint crc = 0;
			var writePageBeforeCrcCheck = _nextWritePage;
			var lastSyncedPageBeforeCrcCheck = _lastSyncedPage;
			var readingPageBeforeCrcCheck = _readingPage;

			for (var i = 0; i < current->PageCount; i++)
			{
				Debug.Assert(_pager.Disposed == false);

				var page = _pager.Read(_readingPage);

				transactionTable[page.PageNumber] = new JournalFile.PagePosition
				{
                    JournalPos = _readingPage,
                    TransactionId = current->TransactionId
				};

				if (page.IsOverflow)
				{
					var numOfPages = _pager.GetNumberOfOverflowPages(page.OverflowSize);
					_readingPage += numOfPages;

					if(checkCrc)
						crc = Crc.Extend(crc, page.Base, 0, numOfPages * AbstractPager.PageSize);
				}
				else
				{
					_readingPage++;
					if (checkCrc)
						crc = Crc.Extend(crc, page.Base, 0, AbstractPager.PageSize);
				}

				_lastSyncedPage = _readingPage - 1;
				_nextWritePage = _lastSyncedPage + 1;
			}

			if (checkCrc && crc != current->Crc)
			{
				RequireHeaderUpdate = true;

				//undo changes to those variables if CRC doesn't match
				_nextWritePage = writePageBeforeCrcCheck;
				_lastSyncedPage = lastSyncedPageBeforeCrcCheck;
				_readingPage = readingPageBeforeCrcCheck;

				return false;
			}

			//update CurrentTransactionHeader _only_ if the CRC check is passed
			LastTransactionHeader = current;
			_transactionPageTranslation = _transactionPageTranslation.SetItems(transactionTable);
		    _transactionEndPositions = _transactionEndPositions.Add(current->TransactionId, _readingPage - 1);
			return true;
		}

	    public void RecoverAndValidate()
		{
			while (ReadOneTransaction())
			{
			}
		}

		public SafeDictionary<long, JournalFile.PagePosition> TransactionPageTranslation
		{
			get { return _transactionPageTranslation; }
		}

        public SafeDictionary<long, long> TransactionEndPositions
        {
            get { return _transactionEndPositions; }
        }

		private bool TryReadAndValidateHeader(out TransactionHeader* current)
		{
			current = (TransactionHeader*)_pager.Read(_readingPage).Base;

			if (current->HeaderMarker != Constants.TransactionHeaderMarker)
			{
                // not a transaction page, 

                // if the header marker is zero, we are probably in the area at the end of the log file, and have no additional log records
                // to read from it. This can happen if the next transaction was too big to fit in the current log file. We stop reading
                // this log file and move to the next one. 

			    RequireHeaderUpdate = current->HeaderMarker != 0;

                return false;
			}

			ValidateHeader(current, LastTransactionHeader);

			if (current->TxMarker.HasFlag(TransactionMarker.Commit) == false)
			{
			    // uncommitted transaction, probably
			    RequireHeaderUpdate = true;
				return false;
			}

			_readingPage++;
			return true;
		}

		private void ValidateHeader(TransactionHeader* current, TransactionHeader* previous)
		{
		    if (current->TransactionId < 0)
		        throw new InvalidDataException("Transaction id cannot be less than 0 (Tx: " + current->TransactionId + " )");
		    if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->LastPageNumber < 0)
		        throw new InvalidDataException("Last page number after committed transaction must be greater than 0");
		    if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->PageCount > 0 && current->Crc == 0)
		        throw new InvalidDataException("Committed and not empty transaction checksum can't be equal to 0");

		    if (previous == null)
		        return;
		    
            if (current->TransactionId != 1 &&
		        // 1 is a first storage transaction which does not increment transaction counter after commit
		        current->TransactionId - previous->TransactionId != 1)
		        throw new InvalidDataException("Unexpected transaction id. Expected: " + (previous->TransactionId + 1) +
		                                       ", got:" + current->TransactionId);
		}

	    public override string ToString()
	    {
	        return _pager.ToString();
	    }
	}
}