using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Voron.Data.BTrees;
using Voron.Global;

namespace Voron.Impl.Journal
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct TransactionHeaderPageInfo
    {
        [FieldOffset(0)]
        public long PageNumber; 

        [FieldOffset(8)]
        public long Size;

        [FieldOffset(16)]
        public bool IsNewDiff;

        [FieldOffset(18)]
        public long DiffSize;
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct TransactionHeader
    {
        public const int SizeOf = 192;
        public static int NonceOffset = (int)Marshal.OffsetOf<TransactionHeader>(nameof(Nonce));
        public static int MacOffset = (int)Marshal.OffsetOf<TransactionHeader>(nameof(Mac));
        public const int NonceSize = 24;

        [FieldOffset(0)]
        public ulong HeaderMarker;

        [FieldOffset(8)]
        public long TransactionId;

        [FieldOffset(16)]
        public long NextPageNumber;

        [FieldOffset(24)]
        public long LastPageNumber;

        [FieldOffset(32)]
        public int PageCount;

        [FieldOffset(36)]
        public TransactionPersistenceModeFlags Flags;

        [FieldOffset(40)]
        public ulong Hash;

        [FieldOffset(48)]
        public TreeRootHeader Root;

        [FieldOffset(110)]
        public TransactionMarker TxMarker;

        [FieldOffset(111)]
        public byte DurableTxIdDeltaAtSubmit;

        [FieldOffset(112)]
        public long CompressedSize;

        [FieldOffset(120)]
        public long UncompressedSize;

        [FieldOffset(128)]
        public long TimeStampTicksUtc; // DateTime.UtcNow.Ticks when the tx happened

        /// <summary>
        /// In a shared journal (root / branch model) - tell us which environment this
        /// transaction belongs to. 
        /// </summary>
        [FieldOffset(136)]
        public Guid JournalId;

        [FieldOffset(152)]
        public fixed byte Nonce[24];

        [FieldOffset(176)]
        public fixed byte Mac[16];



        public override string ToString()
        {
            var validMarker = (HeaderMarker == Constants.TransactionHeaderMarker ? "Valid" : "Invalid");
            var timestamp = new DateTime(TimeStampTicksUtc).ToString("g");
            return $"HeaderMarker: {validMarker}, TransactionId: {TransactionId}, JournalId: {JournalId} NextPageNumber: {NextPageNumber}, LastPageNumber: {LastPageNumber}, " +
                   $"PageCount: {PageCount}, Hash: {Hash}, Root: {Root}, TxMarker: {TxMarker}, CompressedSize: {CompressedSize}," +
                   $" UncompressedSize: {UncompressedSize}, LastDurableTxIdAtSubmit: {LastDurableTxIdAtSubmit}, TimeStamp: {timestamp}";
        }

        public long LastDurableTxIdAtSubmit => DurableTxIdDeltaAtSubmit == 0 ? TransactionId : TransactionId - DurableTxIdDeltaAtSubmit;

        public void SetLastDurableTxIdAtSubmit(long lastDurableTxId)
        {
            var delta = TransactionId - lastDurableTxId;
            if (delta < 0)
                ThrowInvalidLastDurableTxIdAtSubmit(lastDurableTxId, delta);

            // delta too high cannot be represented in a byte, so we will just set it to 0, and trust the recovery for this.
            // > 255 is *really* rare, and adding a corruption handling to recover from this isn't worth it
            DurableTxIdDeltaAtSubmit = delta is 0 or > byte.MaxValue ? (byte)0 : (byte)delta;
        }

        [DoesNotReturn]
        private void ThrowInvalidLastDurableTxIdAtSubmit(long lastDurableTxId, long delta)
        {
            throw new InvalidOperationException(
                $"Cannot record the durability watermark of transaction {TransactionId}: the last durable transaction of its environment is {lastDurableTxId}, " +
                $"which is {-delta} transactions ahead of it. Journal transaction ids are handed out in order under the write lock, " +
                "so a later transaction cannot already be durable - the durability tracking of this environment no longer matches its journal.");
        }
    }
    
    [StructLayout(LayoutKind.Explicit)]
    public struct FreePagesHeader
    {
        [FieldOffset(0)]
        public int NumberOfPages;

        [FieldOffset(4)]
        public int EncodedSectionsSize;
        
        [FieldOffset(8)]
        public int EncodedSectionsCount;
    }
    
    [StructLayout(LayoutKind.Explicit)]
    public struct EncodedFreePagesSection
    {
        [FieldOffset(0)]
        public int Size;
    }
}
