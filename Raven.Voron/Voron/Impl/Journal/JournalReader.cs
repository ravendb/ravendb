using System.Collections.Generic;
using System.IO;
using Voron.Util;

namespace Voron.Impl.Journal
{
	public unsafe class JournalReader
	{
		private readonly IVirtualPager _pager;
		private long _lastSyncedPage;
		private long _writePage;
		private long _readingPage;
		private readonly Dictionary<long, long> _transactionPageTranslation = new Dictionary<long, long>();


		public long WritePage
		{
			get { return _writePage; }
		}

		public long LastSyncedPage
		{
			get { return _lastSyncedPage; }
		}

		public JournalReader(IVirtualPager pager, long startPage, TransactionHeader* previous)
		{
			_pager = pager;
			_readingPage = startPage;
			LastTransactionHeader = previous;

		}

		public TransactionHeader* LastTransactionHeader { get; private set; }

		public bool ReadOneTransaction()
		{
			if (_readingPage >= _pager.NumberOfAllocatedPages)
				return false;

			var current = (TransactionHeader*)_pager.Read(_readingPage).Base;

			if (current->HeaderMarker != Constants.TransactionHeaderMarker)
				return false;

			ValidateHeader(current, LastTransactionHeader);

			if (current->TxMarker.HasFlag(TransactionMarker.Commit) == false)
			{
				_readingPage += current->PageCount + current->OverflowPageCount;
				return false;
			}

			LastTransactionHeader = current;

			_readingPage++;

			uint crc = 0;

			for (var i = 0; i < current->PageCount; i++)
			{
				var page = _pager.Read(_readingPage);

				_transactionPageTranslation[page.PageNumber] = _readingPage;

				if (page.IsOverflow)
				{
					var numOfPages = _pager.GetNumberOfOverflowPages(page.OverflowSize);
					_readingPage += numOfPages;
					crc = Crc.Extend(crc, page.Base, 0, numOfPages * _pager.PageSize);
				}
				else
				{
					_readingPage++;
					crc = Crc.Extend(crc, page.Base, 0, _pager.PageSize);
				}

				_lastSyncedPage = _readingPage - 1;
				_writePage = _lastSyncedPage + 1;
			}

			if (crc != current->Crc)
			{
				throw new InvalidDataException("Checksum mismatch"); //TODO this is temporary, ini the future this condition will just mean that transaction was not committed
			}
			return true;
		}

		public void RecoverAndValidate()
		{
			while (ReadOneTransaction())
			{
			}
		}

		public Dictionary<long, long> TransactionPageTranslation
		{
			get { return _transactionPageTranslation; }
		}

		private void ValidateHeader(TransactionHeader* current, TransactionHeader* previous)
		{
			if (current->TransactionId < 0)
				throw new InvalidDataException("Transaction id cannot be less than 0 (Tx: " + current->TransactionId);
			if (current->TxMarker.HasFlag(TransactionMarker.Start) == false && current->TxMarker.HasFlag(TransactionMarker.Split) == false)
				throw new InvalidDataException("Transaction must have Start or Split marker");
			if (current->TxMarker.HasFlag(TransactionMarker.Commit) && current->LastPageNumber < 0)
				throw new InvalidDataException("Last page number after committed transaction must be greater than 0");
			if (current->PageCount > 0 && current->Crc == 0)
				throw new InvalidDataException("Transaction checksum can't be equal to 0");

			if (previous == null)
				return;

			if (previous->TxMarker.HasFlag(TransactionMarker.Split) && previous->TxMarker.HasFlag(TransactionMarker.Commit))
			{
				if (current->TxMarker.HasFlag(TransactionMarker.Split) && current->TxMarker.HasFlag(TransactionMarker.Commit) && !current->TxMarker.HasFlag(TransactionMarker.Start))
				{
					if (current->TransactionId != previous->TransactionId)
						throw new InvalidDataException("Split transaction should have the same id in the log. Expected id: " +
													   previous->TransactionId + ", got: " + current->TransactionId);
				}
			}
			else if (previous->TxMarker.HasFlag(TransactionMarker.Split))
			{
				if (previous->TxMarker.HasFlag(TransactionMarker.Start))
				{
					if (current->TxMarker.HasFlag(TransactionMarker.Split) == false)
						throw new InvalidDataException("Previous transaction have a Start|Split marker, so the current one should have Split marker too");

					if (current->TransactionId != previous->TransactionId)
						throw new InvalidDataException("Split transaction should have the same id in the log. Expected id: " +
													   previous->TransactionId + ", got: " + current->TransactionId);
				}
			}
			else if (previous->TxMarker.HasFlag(TransactionMarker.Commit))
			{
				if (current->TransactionId != 1 && // 1 is a first storage transaction which does not increment transaction counter after commit
				   current->TransactionId - previous->TransactionId != 1)
					throw new InvalidDataException("Unexpected transaction id. Expected: " + (previous->TransactionId + 1) + ", got:" +
												   current->TransactionId);
			}
		}
	}
}