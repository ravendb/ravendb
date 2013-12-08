using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
	using Lz4Net;

	public unsafe class JournalReader
	{
		private readonly IVirtualPager _pager;
		private readonly IVirtualPager _recoveryPager;

		private readonly long _lastSyncedTransactionId;
		private long _lastSyncedPage;
		private long _nextWritePage;
		private long _readingPage;
		private int _recoveryPage;

		private LinkedDictionary<long, JournalFile.PagePosition> _transactionPageTranslation = LinkedDictionary<long, JournalFile.PagePosition>.Empty;

		public bool RequireHeaderUpdate { get; private set; }

		public bool EncounteredStopCondition { get; private set; }

		public long NextWritePage
		{
			get { return _nextWritePage; }
		}

		public JournalReader(IVirtualPager pager, IVirtualPager recoveryPager, long lastSyncedTransactionId, TransactionHeader* previous)
		{
			RequireHeaderUpdate = false;
			_pager = pager;
			_recoveryPager = recoveryPager;
			_lastSyncedTransactionId = lastSyncedTransactionId;
			_readingPage = 0;
			_recoveryPage = 0;
			_nextWritePage = 0;
			LastTransactionHeader = previous;
		}

		public TransactionHeader* LastTransactionHeader { get; private set; }

		public bool ReadOneTransaction(bool checkCrc = true)
		{
			if (_readingPage >= _pager.NumberOfAllocatedPages)
				return false;

			var transactionTable = new Dictionary<long, JournalFile.PagePosition>();

			TransactionHeader* current;
			if (!TryReadAndValidateHeader(out current)) 
				return false;

			var compressedPages = (current->CompressedSize / AbstractPager.PageSize) + (current->CompressedSize % AbstractPager.PageSize == 0 ? 0 : 1);

			if (current->TransactionId <= _lastSyncedTransactionId)
			{
				_readingPage += compressedPages;
				return true; // skipping
			}

			uint crc = 0;
			var writePageBeforeCrcCheck = _nextWritePage;
			var lastSyncedPageBeforeCrcCheck = _lastSyncedPage;
			var readingPageBeforeCrcCheck = _readingPage;

			_recoveryPager.EnsureContinuous(null, _recoveryPage, (current->PageCount + current->OverflowPageCount) + 1);
			var dataPage = _recoveryPager.GetWritable(_recoveryPage);

			NativeMethods.memset(dataPage.Base, 0, (current->PageCount + current->OverflowPageCount) * AbstractPager.PageSize);
			var compressedSize = Lz4.LZ4_uncompress(_pager.Read(_readingPage).Base, dataPage.Base, current->UncompressedSize);

			if (compressedSize != current->CompressedSize) //Compression error. Probably file is corrupted
			{
				RequireHeaderUpdate = true;

				return false; 
			}
				

			for (var i = 0; i < current->PageCount; i++)
			{
				Debug.Assert(_pager.Disposed == false);
				Debug.Assert(_recoveryPager.Disposed == false);

				var page = _recoveryPager.Read(_recoveryPage);

				transactionTable[page.PageNumber] = new JournalFile.PagePosition
				{
					JournalPos = _recoveryPage,
                    TransactionId = current->TransactionId
				};

				if (page.IsOverflow)
				{
					var numOfPages = _recoveryPager.GetNumberOfOverflowPages(page.OverflowSize);
					_recoveryPage += numOfPages;

					if(checkCrc)
						crc = Crc.Extend(crc, page.Base, 0, numOfPages * AbstractPager.PageSize);
				}
				else
				{
					_recoveryPage++;
					if (checkCrc)
						crc = Crc.Extend(crc, page.Base, 0, AbstractPager.PageSize);
				}

				_lastSyncedPage = _recoveryPage - 1;
				_nextWritePage = _lastSyncedPage + 1;
			}

			_readingPage += compressedPages;

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
			return true;
		}

	    public void RecoverAndValidate()
		{
			while (ReadOneTransaction())
			{
			}
		}

		public LinkedDictionary<long, JournalFile.PagePosition> TransactionPageTranslation
		{
			get { return _transactionPageTranslation; }
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