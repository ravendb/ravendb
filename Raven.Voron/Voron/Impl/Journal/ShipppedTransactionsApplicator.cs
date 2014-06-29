using System;
using System.IO;
using Voron.Impl.Paging;
using Voron.Util;

namespace Voron.Impl.Journal
{
    public unsafe class ShipppedTransactionsApplicator : IDisposable
    {
        private readonly StorageEnvironment _env;
        private uint _previousTransactionCrc;
        private long _previousTransaction;

        public ShipppedTransactionsApplicator(StorageEnvironment env)
        {
            _env = env;

            // todo: read previous tx crc
            // todo: read previous transaction id
        }

        public void ApplyShippedLogs(byte* txPages, int pageCount)
        {
            var transactionHeader = (TransactionHeader*)txPages;
            //todo: uncompress the data & validate crc & previous transaction crc & transaction id vs prev transaction id
            //then write the _compressed_ data directly to disk
            //and apply them to the data directly by creating a virtual (just a number) transaction
            // no need to do a decompress, commit, compress cycle
            using (var tx = _env.NewTransaction(TransactionFlags.ReadWrite))
            {
                TransactionHeader txHeader = shippedTransactions.Header;
                var compressedPages = (txHeader.CompressedSize / AbstractPager.PageSize) + (txHeader.CompressedSize % AbstractPager.PageSize == 0 ? 0 : 1);

                fixed (byte* txPagesPtr = (byte[]) shippedTransactions.CopiedPages)
                {
                    var crc = Crc.Value(txPagesPtr, 0, compressedPages * AbstractPager.PageSize);
                    if (txHeader.Crc != crc)
                        throw new InvalidDataException("Invalid CRC signature for shipped transaction " + txHeader.TransactionId);

                    if (_previousTransactionCrc != shippedTransactions.PreviousTransactionCrc)
                        throw new InvalidDataException("Invalid CRC signature for previous shipped transaction " + txHeader.TransactionId + ", is there a break in the chain?");

                    if (txHeader.TransactionId - 1 != _previousTransaction)
                        throw new InvalidDataException("Invalid id for shipped transaction got " + txHeader.TransactionId + " but expected " + (_previousTransaction + 1) + ", is there a break in the chain?");

                    var totalPages = txHeader.PageCount + txHeader.OverflowPageCount;
                    var pageFromScratchBuffer = _env.ScratchBufferPool.Allocate(tx, totalPages);
                    try
                    {
                        var dest = _env.ScratchBufferPool.AcquirePagePointer(pageFromScratchBuffer.PositionInScratchBuffer);
                        LZ4.Decode64(txPagesPtr, txHeader.CompressedSize, dest, txHeader.UncompressedSize, true);
                        _previousTransactionCrc = crc;
                    }
                    catch (Exception e)
                    {
                        _env.ScratchBufferPool.Free(pageFromScratchBuffer.PositionInScratchBuffer, -1);
                        throw new InvalidDataException("Could not de-compress shipped transaction pages, invalid data", e);
                    }
                }
                tx.WriteDirect(pageFromScratchBuffer);
                tx.Commit();
            }
        }

        public void Dispose()
        {
        }
    }
}