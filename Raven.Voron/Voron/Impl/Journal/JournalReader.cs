using System;
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
        private readonly IVirtualPager _recoveryPager;

        private readonly long _lastSyncedTransactionId;
        private long _readingPage;

        private readonly Dictionary<long, JournalFile.PagePosition> _transactionPageTranslation = new Dictionary<long, JournalFile.PagePosition>();
        private int _recoveryPage;

        public bool RequireHeaderUpdate { get; private set; }

        public long NextWritePage
        {
            get { return _readingPage; }
        }

        public JournalReader(IVirtualPager pager, IVirtualPager recoveryPager, long lastSyncedTransactionId, TransactionHeader* previous)
        {
            RequireHeaderUpdate = false;
            _pager = pager;
            _recoveryPager = recoveryPager;
            _lastSyncedTransactionId = lastSyncedTransactionId;
            _readingPage = 0;
            _recoveryPage = 0;
            LastTransactionHeader = previous;
        }

        public TransactionHeader* LastTransactionHeader { get; private set; }

        public bool ReadOneTransaction(StorageEnvironmentOptions options,bool checkCrc = true)
        {
            if (_readingPage >= _pager.NumberOfAllocatedPages)
                return false;

            TransactionHeader* current;
            if (!TryReadAndValidateHeader(options, out current))
                return false;

            var compressedPages = (current->CompressedSize / AbstractPager.PageSize) + (current->CompressedSize % AbstractPager.PageSize == 0 ? 0 : 1);

            if (current->TransactionId <= _lastSyncedTransactionId)
            {
                _readingPage += compressedPages;
                return true; // skipping
            }

	        if (checkCrc)
	        {
		        uint crc = Crc.Value(_pager.Read(_readingPage).Base, 0, compressedPages * AbstractPager.PageSize);

				if (crc != current->Crc)
				{
					RequireHeaderUpdate = true;
					options.InvokeRecoveryError(this, "Invalid CRC signature for transaction " + current->TransactionId, null);

					return false;
				}
	        }

            _recoveryPager.EnsureContinuous(null, _recoveryPage, (current->PageCount + current->OverflowPageCount) + 1);
            var dataPage = _recoveryPager.GetWritable(_recoveryPage);

            NativeMethods.memset(dataPage.Base, 0, (current->PageCount + current->OverflowPageCount) * AbstractPager.PageSize);
            try
            {
                LZ4.Decode64(_pager.AcquirePagePointer(_readingPage), current->CompressedSize, dataPage.Base, current->UncompressedSize, true);
            }
            catch (Exception e)
            {
                options.InvokeRecoveryError(this, "Could not de-compress, invalid data", e);
                RequireHeaderUpdate = true;

                return false;   
            }

            var tempTransactionPageTranslaction = new Dictionary<long, JournalFile.PagePosition>();

            for (var i = 0; i < current->PageCount; i++)
            {
                Debug.Assert(_pager.Disposed == false);
                Debug.Assert(_recoveryPager.Disposed == false);

                var page = _recoveryPager.Read(_recoveryPage);

				 tempTransactionPageTranslaction[page.PageNumber] = new JournalFile.PagePosition
                {
                    JournalPos = _recoveryPage,
                    TransactionId = current->TransactionId
                };

                if (page.IsOverflow)
                {
                    var numOfPages = _recoveryPager.GetNumberOfOverflowPages(page.OverflowSize);
                    _recoveryPage += numOfPages;
                }
                else
                {
                    _recoveryPage++;
                }
            }

            _readingPage += compressedPages;

            LastTransactionHeader = current;
			
            foreach (var pagePosition in tempTransactionPageTranslaction)
            {
                _transactionPageTranslation[pagePosition.Key] = pagePosition.Value;
            }

            return true;
        }

        public void RecoverAndValidate(StorageEnvironmentOptions options)
        {
            while (ReadOneTransaction(options))
            {
            }
        }

        public Dictionary<long, JournalFile.PagePosition> TransactionPageTranslation
        {
            get { return _transactionPageTranslation; }
        }

        private bool TryReadAndValidateHeader(StorageEnvironmentOptions options,out TransactionHeader* current)
        {
            current = (TransactionHeader*)_pager.Read(_readingPage).Base;

            if (current->HeaderMarker != Constants.TransactionHeaderMarker)
            {
                // not a transaction page, 

                // if the header marker is zero, we are probably in the area at the end of the log file, and have no additional log records
                // to read from it. This can happen if the next transaction was too big to fit in the current log file. We stop reading
                // this log file and move to the next one. 

				RequireHeaderUpdate = current->HeaderMarker != 0;
                if (RequireHeaderUpdate)
                {
                    options.InvokeRecoveryError(this,
                        "Transaction " + current->TransactionId +
                        " header marker was set to garbage value, file is probably corrupted", null);
                }

                return false;
            }

            ValidateHeader(current, LastTransactionHeader);

            if (current->TxMarker.HasFlag(TransactionMarker.Commit) == false)
            {
                // uncommitted transaction, probably
                RequireHeaderUpdate = true;
                options.InvokeRecoveryError(this,
                        "Transaction " + current->TransactionId +
                        " was not committed", null);
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
			if (current->Compressed)
			{
				if (current->CompressedSize <= 0)
					throw new InvalidDataException("Compression error in transaction.");
			} else
				throw new InvalidDataException("Uncompressed transactions are not supported.");

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