// -----------------------------------------------------------------------
//  <copyright file="PrefixComparisonCache.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

namespace Voron
{
    public class PrefixCompareResult
    {
        public long PageNumber;
        public byte PrefixId;
        public ushort ComparedBytes;
        public int CompareResult;
    }

    public class PrefixComparisonCache
    {
        private PrefixCompareResult _cachedItem;
        public bool Disabled = false;

        public void SetPrefixComparisonResult(byte prefixId, long pageNumber, ushort comparedBytes, int cmpResult)
        {
            if(Disabled)
                return;

            if (_cachedItem == null)
            {
                _cachedItem = new PrefixCompareResult()
                {
                    PageNumber = pageNumber,
                    PrefixId = prefixId,
                    CompareResult = cmpResult,
                    ComparedBytes = comparedBytes
                };
            }
            else
            {
                _cachedItem.PageNumber = pageNumber;
                _cachedItem.PrefixId = prefixId;
                _cachedItem.CompareResult = cmpResult;
                _cachedItem.ComparedBytes = comparedBytes;
            }
        }

        public bool TryGetCachedResult(byte prefixId, long pageNumber, ushort bytesToCompare, out int result)
        {
            if (_cachedItem == null || Disabled)
            {
                result = int.MinValue;
                return false;
            }

            if (_cachedItem.PageNumber != pageNumber)
            {
                _cachedItem = null;
                result = int.MinValue;
                return false;
            }

            if (_cachedItem.PrefixId != prefixId)
            {
                result = int.MinValue;
                return false;
            }

            if(_cachedItem.ComparedBytes != bytesToCompare)
            {
                if (_cachedItem.ComparedBytes > bytesToCompare && _cachedItem.CompareResult == 0)
                {
                    result = _cachedItem.CompareResult;
                    return true;
                }

                result = int.MinValue;
                return false;
            }

            result = _cachedItem.CompareResult;
            return true;
        }
    }
}
