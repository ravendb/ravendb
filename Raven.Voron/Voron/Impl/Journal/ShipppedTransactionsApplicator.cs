using System;
using System.IO;
using System.Linq;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class ShipppedTransactionsApplicator : IDisposable
    {
        private readonly StorageEnvironment _env;
        private uint _previousTransactionCrc;
        private long _previousTransaction;

		public ShipppedTransactionsApplicator(StorageEnvironment env, long previousTransaction, uint previousTransactionCrc)
        {
	        _env = env;
	        _previousTransaction = previousTransaction;
			_previousTransactionCrc = previousTransactionCrc;

        }

	    public event Action<long, uint> TransactionApplied;

	    public long PreviousTransaction
	    {
		    get { return _previousTransaction; }
	    }
	    public uint PreviousTransactionCrc
	    {
		    get { return _previousTransactionCrc; }
	    }

	    public void ApplyShippedLog(byte[] txPagesRaw, uint previousTransactionCrc)
	    {
		    
			//todo: uncompress the data & validate crc & previous transaction crc & transaction id vs prev transaction id
		    //then write the _compressed_ data directly to disk
		    //and apply them to the data directly by creating a virtual (just a number) transaction
		    // no need to do a decompress, commit, compress cycle


			fixed (byte* pages = txPagesRaw)
		    {
			    using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
			    {
				    var transactionHeader = (TransactionHeader*) pages;
				    var dataPages = pages + AbstractPager.PageSize;

				    var compressedPages = (transactionHeader->CompressedSize/AbstractPager.PageSize) + (transactionHeader->CompressedSize%AbstractPager.PageSize == 0 ? 0 : 1);
				    var crc = Crc.Value(dataPages, 0, compressedPages*AbstractPager.PageSize);

				    if (transactionHeader->Crc != crc)
					    throw new InvalidDataException("Invalid CRC signature for shipped transaction " + transactionHeader->TransactionId);

				    if (transactionHeader->TransactionId - 1 != PreviousTransaction)
					    throw new InvalidDataException("Invalid id for shipped transaction got " + transactionHeader->TransactionId + " but expected " + (PreviousTransaction + 1) + ", is there a break in the chain?");

			
				    if (PreviousTransactionCrc != previousTransactionCrc)
					    throw new InvalidDataException("Invalid CRC signature for previous shipped transaction " + transactionHeader->TransactionId + ", is there a break in the chain?");

				    var totalPages = transactionHeader->PageCount + transactionHeader->OverflowPageCount;
				    
					var decompressBuffer = _env.ScratchBufferPool.Allocate(tx, totalPages);
				    try
				    {
					    try
					    {
						    var dest = _env.ScratchBufferPool.AcquirePagePointer(decompressBuffer.PositionInScratchBuffer);
						    LZ4.Decode64(dataPages, transactionHeader->CompressedSize, dest, transactionHeader->UncompressedSize, true);
					    }
					    catch (Exception e)
					    {
						    throw new InvalidDataException("Could not de-compress shipped transaction pages, invalid data", e);
					    }

						tx.WriteDirect(transactionHeader, decompressBuffer);
				    }
				    catch (Exception)
				    {
						_env.ScratchBufferPool.Free(decompressBuffer.PositionInScratchBuffer, -1);
					    throw;
				    }
				    tx.Commit();

					OnTransactionApplied(transactionHeader->TransactionId, crc);
					_previousTransactionCrc = crc;
					_previousTransaction = transactionHeader->TransactionId;
			    }

		    }
	    }

	    protected void OnTransactionApplied(long previousTransactionId, uint previousTransactionCrc)
	    {
		    var transactionApplied = TransactionApplied;
		    if (transactionApplied != null)
			    transactionApplied(previousTransactionId, previousTransactionCrc);
	    }

	    public void Dispose()
        {
        }

	    internal void UpdatePreviousTransaction(long transactionId)
	    {
			_previousTransaction = transactionId;
	    }

	    internal void UpdatePreviousTransactionCrc(uint crc)
	    {
			_previousTransactionCrc = crc;
	    }
    }
}