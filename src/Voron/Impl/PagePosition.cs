// -----------------------------------------------------------------------
//  <copyright file="PagePosition.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Sparrow;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Voron.Impl
{

    public class PagePositionEqualityComparer : IEqualityComparer<PagePosition>
    {
        public static readonly PagePositionEqualityComparer Instance = new PagePositionEqualityComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(PagePosition x, PagePosition y)
        {
            if (x == y) return true;
            if (x == null || y == null) return false;

            return x.ScratchPos == y.ScratchPos && x.TransactionId == y.TransactionId && x.JournalNumber == y.JournalNumber && x.IsFreedPageMarker == y.IsFreedPageMarker && x.ScratchNumber == y.ScratchNumber;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(PagePosition obj)
        {
            long v = Hashing.Combine(obj.ScratchPos, obj.TransactionId);
            long w = Hashing.Combine(obj.JournalNumber, (long) obj.ScratchNumber);
            return (int) (v ^ w);
        }
    }

    public class PagePosition
    {
        public readonly long ScratchPos;
        public readonly long TransactionId;
        public readonly long JournalNumber;
        public readonly int ScratchNumber;
        public readonly bool IsFreedPageMarker;
        public bool UnusedInPTT;

        public PagePosition(long scratchPos, long transactionId, long journalNumber, int scratchNumber, bool isFreedPageMarker = false)
        {
            this.ScratchPos = scratchPos;
            this.TransactionId = transactionId;
            this.JournalNumber = journalNumber;
            this.ScratchNumber = scratchNumber;
            this.IsFreedPageMarker = isFreedPageMarker;
        }

        public override bool Equals(object obj)
        {
            return PagePositionEqualityComparer.Instance.Equals(this, obj as PagePosition);
        }

        public override int GetHashCode()
        {
            return PagePositionEqualityComparer.Instance.GetHashCode(this);
        }

        public override string ToString()
        {
            return $"ScratchPos: {ScratchPos}, TransactionId: {TransactionId}, JournalNumber: {JournalNumber}, ScratchNumber: {ScratchNumber}, IsFreedPageMarker: {IsFreedPageMarker}";
        }
    }
}
