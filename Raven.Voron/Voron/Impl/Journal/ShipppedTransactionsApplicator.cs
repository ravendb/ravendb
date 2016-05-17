using System;
using System.IO;
using System.Linq;
using System.Threading;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class ShipppedTransactionsApplicator : IDisposable
    {
        private readonly StorageEnvironment _env;
        private uint _previousTransactionCrc;
        private long _previousTransactionId;
        public ShipppedTransactionsApplicator(StorageEnvironment env, uint previousTransactionCrc, long previousTransactionId)
        {
            _env = env;
            _previousTransactionCrc = previousTransactionCrc;
            _previousTransactionId = previousTransactionId;
        }

        public event Action<long, uint> TransactionApplied;
        
        public long PreviousTransactionId
        {
            get { return _previousTransactionId; }
        }

        public uint PreviousTransactionCrc
        {
            get { return _previousTransactionCrc; }
        }

        public void SetPreviousTransaction(long transactionId, uint previousTransactionCrc)
        {
            _previousTransactionId = transactionId;
            _previousTransactionCrc = previousTransactionCrc;
        }

        public void ApplyShippedLog(byte[] txPagesRaw)
        {
            fixed (byte* pages = txPagesRaw)
            {
                using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
                {
                    var transactionHeader = (TransactionHeader*) pages;
                    var dataPages = pages + AbstractPager.PageSize;

                    var compressedPages = (transactionHeader->CompressedSize/AbstractPager.PageSize) + (transactionHeader->CompressedSize%AbstractPager.PageSize == 0 ? 0 : 1);
                    var crc = Crc.Value(dataPages, 0, compressedPages*AbstractPager.PageSize);

                    var transactionId = transactionHeader->TransactionId;
                    if (transactionHeader->Crc != crc)
                        throw new InvalidDataException("Invalid CRC signature for shipped transaction " + transactionId);

                    if (transactionId - 1 != PreviousTransactionId)
                        throw new InvalidDataException("Invalid id for shipped transaction got " + transactionId + " but expected " + (PreviousTransactionId + 1) + ", is there a break in the chain?");

                    if (transactionHeader->PreviousTransactionCrc != PreviousTransactionCrc)
                        throw new InvalidDataException("Invalid CRC signature for previous shipped transaction " + transactionId + ", is there a break in the chain?");

                    var totalPages = transactionHeader->PageCount + transactionHeader->OverflowPageCount;
                    
                    var decompressBuffer = _env.ScratchBufferPool.Allocate(tx, totalPages);
                    try
                    {
                        try
                        {
                            var dest = _env.ScratchBufferPool.AcquirePagePointer(tx, decompressBuffer.ScratchFileNumber, decompressBuffer.PositionInScratchBuffer);
                            LZ4.Decode64(dataPages, transactionHeader->CompressedSize, dest, transactionHeader->UncompressedSize, true);
                        }
                        catch (Exception e)
                        {
                            throw new InvalidDataException("Could not de-compress shipped transaction pages, invalid data", e);
                        }

                        tx.WriteDirect(transactionHeader, decompressBuffer);
                    
                        _previousTransactionCrc = crc;
                        _previousTransactionId = transactionHeader->TransactionId;
                    }
                    finally 
                    {
                        _env.ScratchBufferPool.Free(decompressBuffer.ScratchFileNumber, decompressBuffer.PositionInScratchBuffer, -1);
                    }
                    tx.Commit();

                    OnTransactionApplied(transactionId, crc);
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
    }
}
