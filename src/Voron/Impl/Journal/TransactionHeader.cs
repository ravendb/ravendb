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
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct TransactionHeader
    {
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
        public int OverflowPageCount;

        [FieldOffset(40)]
        public ulong Hash;

        [FieldOffset(48)]
        public TreeRootHeader Root;

        [FieldOffset(110)]
        public TransactionMarker TxMarker;

        [FieldOffset(111)]
        public bool Compressed;

        [FieldOffset(112)]
        public int CompressedSize;

        [FieldOffset(116)]
        public int UncompressedSize;

        [FieldOffset(120)]
        public long TimeStampTicksUtc; // DateTime.UtcNow.Ticks when the tx happened

        public override string ToString()
        {
            var validMarker = (HeaderMarker == Constants.TransactionHeaderMarker ? "Valid" : "Invalid");
            var timestamp = new DateTime(TimeStampTicksUtc).ToString("g");
            return $"HeaderMarker: {validMarker}, TransactionId: {TransactionId}, NextPageNumber: {NextPageNumber}, LastPageNumber: {LastPageNumber}, PageCount: {PageCount}, OverflowPageCount: {OverflowPageCount}, Hash: {Hash}, Root: {Root}, TxMarker: {TxMarker}, Compressed: {Compressed}, CompressedSize: {CompressedSize}, UncompressedSize: {UncompressedSize}, TimeStamp: {timestamp}";
        }
    }
}