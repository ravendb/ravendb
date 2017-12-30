// -----------------------------------------------------------------------
//  <copyright file="TransactionHeader.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.InteropServices;
using Voron.Data.BTrees;
using Voron.Global;

namespace Voron.Impl.Journal
{
    [StructLayout(LayoutKind.Explicit)]
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
        public bool Reserved2;

        [FieldOffset(112)]
        public long CompressedSize;

        [FieldOffset(120)]
        public long UncompressedSize;

        [FieldOffset(128)]
        public long TimeStampTicksUtc; // DateTime.UtcNow.Ticks when the tx happened

        [FieldOffset(120)]
        public fixed byte Reserved[16];

        [FieldOffset(152)]
        public fixed byte Nonce[24];

        [FieldOffset(176)]
        public fixed byte Mac[16];



        public override string ToString()
        {
            var validMarker = (HeaderMarker == Constants.TransactionHeaderMarker ? "Valid" : "Invalid");
            var timestamp = new DateTime(TimeStampTicksUtc).ToString("g");
            return $"HeaderMarker: {validMarker}, TransactionId: {TransactionId}, NextPageNumber: {NextPageNumber}, LastPageNumber: {LastPageNumber}, " +
                   $"PageCount: {PageCount}, Hash: {Hash}, Root: {Root}, TxMarker: {TxMarker}, CompressedSize: {CompressedSize}," +
                   $" UncompressedSize: {UncompressedSize}, TimeStamp: {timestamp}";
        }
    }
}
