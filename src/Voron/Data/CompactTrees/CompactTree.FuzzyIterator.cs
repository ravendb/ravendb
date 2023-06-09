using System;
using System.Diagnostics;
using Sparrow.Server.Strings;
using Voron.Data.Lookups;

namespace Voron.Data.CompactTrees;

partial class CompactTree
{
    private static int DictionaryOrder(ReadOnlySpan<byte> s1, ReadOnlySpan<byte> s2)
    {
        // Bed - Tree: An All-Purpose Index Structure for String Similarity Search Based on Edit Distance
        // https://event.cwi.nl/SIGMOD-RWE/2010/12-16bf4c/paper.pdf

        //  Intuitively, such a sorting counts the total number of strings with length smaller than |s|
        //  plus the number of string with length equal to |s| preceding s in dictionary order.

        int len1 = s1.Length;
        int len2 = s2.Length;

        if (len1 == 0 && len2 == 0)
            return 0;

        if (len1 == 0)
            return -1;

        if (len2 == 0)
            return 1;

        //  Given two strings si and sj, it is sufficient to find the most significant position p where the two string differ
        int minLength = len1 < len2 ? len1 : len2;

        // If π(si[p]) < π(sj[p]), we can assert that si precedes sj in dictionary order φd, and viceversa.
        int i;
        for (i = 0; i < minLength; i++)
        {
            if (s1[i] < s2[i])
                return -1;
            if (s2[i] < s1[i])
                return 1;
        }

        if (len1 < len2)
            return -1;

        return 1;
    }

    private (int Match, int SearchPos) FuzzySearchInCurrentPage(in CompactKey key, Lookup<CompactKeyLookup>.PageRef pageRef)
    {
        var encodedKey = key.EncodedWithCurrent(out var _);

        int high = pageRef.NumberOfEntries - 1, low = 0;
        int match = -1;
        int mid = 0;
        while (low <= high)
        {
            mid = (high + low) / 2;

            var currentKey = pageRef.GetKey(mid);

            match = DictionaryOrder(encodedKey, currentKey.GetKey(_inner).Decoded());

            if (match == 0)
            {
                return (0, mid);
            }

            if (match > 0)
            {
                low = mid + 1;
                match = 1;
            }
            else
            {
                high = mid - 1;
                match = -1;
            }
        }

        match = match > 0 ? 1 : -1;
        if (match > 0)
            mid++;
        return (match, ~mid);
    }

    private void FuzzySearchPageAndPushNext(CompactKey key, Lookup<CompactKeyLookup>.PageRef pageRef)
    {
        (int match, int searchPos) = FuzzySearchInCurrentPage(key, pageRef);

        if (searchPos < 0)
            searchPos = ~searchPos;
        if (match != 0 && searchPos > 0)
            searchPos--; // went too far

        int actualPos = Math.Min(pageRef.NumberOfEntries - 1, searchPos);
        var nextPage = pageRef.GetValue(actualPos);

        pageRef.PushPage(nextPage);
    }

    private void FuzzyFindPageFor(ReadOnlySpan<byte> key)
    {
        // Algorithm 2: Find Node
        _inner.InitializeCursorState();
        var pageRef = _inner.GetPageRef();

        using var scope = new CompactKeyCacheScope(_inner.Llt, key, _inner.State.DictionaryId);
        var encodedKey = scope.Key;
        while (pageRef.IsBranch)
        {
            FuzzySearchPageAndPushNext(encodedKey, pageRef);
        }
    }

    public FuzzyIterator FuzzyIterate(Slice fuzzyKey, float distance)
    {
        return new FuzzyIterator(this, fuzzyKey, distance);
    }

    public struct FuzzyIterator
    {
        private Slice _baseKey;
        private readonly float _minScore;
        private int _posInPage;
        private readonly CompactTree _tree;

        public FuzzyIterator(CompactTree tree, Slice baseKey, float maxDistance)
        {
            _baseKey = baseKey;
            _minScore = maxDistance;
            _tree = tree;
        }

        public void Reset()
        {
            Seek(_baseKey);
        }

        public void Seek(ReadOnlySpan<byte> key)
        {
            _tree.FuzzyFindPageFor(key);
            _posInPage = 0;
        }

        public bool MoveNext(out CompactKey compactKey, out long value, out float score)
        {
            LevenshteinDistance distance = default;

            var pageReg = _tree._inner.GetPageRef();
            
            byte firstLetter = _baseKey[0];

            while (true)
            {
                if (_posInPage < pageReg.NumberOfEntries) // same page
                {
                    value = pageReg.GetValue(_posInPage);
                    compactKey = pageReg.GetKey(_posInPage).GetKey(_tree._inner);
                    var key = compactKey.Decoded();

                    _posInPage++;
                    
                    float currentScore = distance.GetDistance(_baseKey, key);
                    if (currentScore < _minScore)
                        continue;

                    // PERF: This bound can probably be improved by being clever at the tree level, but without understanding
                    // the actual performance impact of this bounding on real datasets it is a moot point to worry about it

                    // There is no longer a common denominator (LCP). This in effect will not accept cases where the potential
                    // solutions do not share the first letter. We can fix it by finding the max key and doing a range query.
                    // https://event.cwi.nl/SIGMOD-RWE/2010/12-16bf4c/paper.pdf
                    if (key[0] != firstLetter)
                        goto IsDone;

                    score = currentScore;
                    return true;
                }

                if (pageReg.GoToNextPage() == false)
                    goto IsDone;
            }

            IsDone:
            compactKey = default;
            value = default;
            score = 0;
            return false;
        }
    }
}
