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

            return x.ScratchPos == y.ScratchPos && x.TransactionId == y.TransactionId && x.JournalNumber == y.JournalNumber && x.IsFreedPageMarker == y.IsFreedPageMarker && x.ScratchNumber == y.ScratchNumber; ;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetHashCode(PagePosition obj)
        {
            int v = Hashing.Combine(obj.ScratchPos.GetHashCode(), obj.TransactionId.GetHashCode());
            int w = Hashing.Combine(obj.JournalNumber.GetHashCode(), obj.ScratchNumber.GetHashCode());
            return Hashing.Combine(obj.IsFreedPageMarker ? 1 : 0, Hashing.Combine(v, w));
        }
    }

    public class PagePosition
    {
        public long ScratchPos;
        public long TransactionId;
        public long JournalNumber;
        public int ScratchNumber;
        public bool IsFreedPageMarker;
        public bool UnusedInPTT;

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
            return string.Format("ScratchPos: {0}, TransactionId: {1}, JournalNumber: {2}, ScratchNumber: {3}, IsFreedPageMarker: {4}", ScratchPos, TransactionId, JournalNumber, ScratchNumber, IsFreedPageMarker);
        }
    }
}
