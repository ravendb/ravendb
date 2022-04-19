using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;

namespace Voron.Data.CompactTrees;


// The heuristics to rebuild the compression dictionaries can be tricky. 
// There are many ways to regenerate dictionaries to obtain better compression, but they are always a gamble.
// The most important decision to make is either to use global scopes or to use local scopes.
// Global scopes may be the best decision, because they are the most likely to be successful in the long run.
// However, they are also the most expensive to build.
// Local scopes are less likely to succeed, but they are cheaper to build. The way to ensure high levels of
// compression is to create as many dictionaries specialized for each page. This is prohibitively expensive,
// and the tradeoff is that it is not possible to use a unique dictionary for each page. Finding the right
// heuristic for each clusters of keys is a very difficult problem. Therefore, when using local scopes we
// should be careful where the boundaries of each dictionary is used; which in return would increase the runtime
// work needed to be done for the housekeeping.
// 
// For local scopes we could heuristically create dictionaries during splitting based on the data that we encounter.
//  - Pro: the dictionary will have a tighter compression ratio,
//  - Pro: we could avoid splitting pages just upgrading the dictionary,
//  - Cons: many more dictionaries to track, higher performance burden on page splits.
// 
// For global scope dictionaries we could perform offline sampling of the whole database as a maintenance tasks
// after ceratin conditions are met (like doubling the amount of keys) but build a single dictionary that can
// compress the whole tree in the most efficient way (if needed).
//  - Pro: There is only a single active dictionary that everybody uses,
//  - Pro: The performance burden could be paid as an idle task and happen at the moment of splitting,
//  - Pro: The pages could be upgraded over time when performing writes on a particular page to amortize the IO cost,
//  - Pro: Random selection or fixed budget would do the trick of adaptively upgrading the tree compression over time,
//  - Cons: Requires to have a task to perform the dictionary creation outside of the insertion process,
//  - Cons: Compression ratio will not be THAT tight per single page,
//  - Cons: We may need larger dictionaries to accomodate better compression ratios. 
//
//  In Voron we use global scopes dictionaries because they have a cleaner implementation with less intrusive processes
//  where it really matters (writing). 

unsafe partial class CompactTree
{
    public struct FullDictionaryKeyScanner : IReadOnlySpanEnumerator
    {
        private readonly CompactTree _tree;
        private Iterator _iterator;

        public FullDictionaryKeyScanner(CompactTree tree)
        {
            _tree = tree;
            _iterator = _tree.Iterate();
            _iterator.Reset();
        }

        public bool MoveNext(out ReadOnlySpan<byte> result)
        {
            bool operationResult = _iterator.MoveNext(out Span<byte> resultSlice, out long _);
            result = resultSlice;
            return operationResult;
        }

        public void Reset()
        {
            _iterator = _tree.Iterate();
            _iterator.Reset();
        }
    }

    public struct RandomDictionaryKeyScanner : IReadOnlySpanEnumerator
    {
        private readonly CompactTree _tree;
        private readonly int _samples;
        private readonly int _seed;
        private RandomIterator _iterator;

        public RandomDictionaryKeyScanner(CompactTree tree, int samples, int seed = 0)
        {
            _tree = tree;
            _seed = seed;
            _samples = samples;
            _iterator = _tree.RandomIterate(samples, seed: seed);
            _iterator.Reset();
        }

        public bool MoveNext(out ReadOnlySpan<byte> result)
        {
            bool operationResult = _iterator.MoveNext(out Span<byte> resultSlice, out long _);
            result = resultSlice;
            return operationResult;
        }

        public void Reset()
        {
            _iterator = _tree.RandomIterate(_samples, seed: _seed);
            _iterator.Reset();
        }
    }

    public struct RandomBranchDictionaryKeyScanner : IReadOnlySpanEnumerator
    {
        private readonly long _samples;        
        private readonly Random _generator;
        private readonly CompactTree _tree;
        private int _currentSample;
        private int _currentIdx;
        private IteratorCursorState _cursor;
        private HashSet<long> _pagesSeen;

        public RandomBranchDictionaryKeyScanner(CompactTree tree, long samples, int seed = 0)
        {
            _tree = tree;
            _samples = samples;
            _currentSample = 0;
            _currentIdx = 0;
            _generator = (seed == 0) ? new() : new(seed);
            _pagesSeen = new HashSet<long>();

            _cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };
            _tree.PushPage(_tree._state.RootPage, ref _cursor);            
        }

        public bool MoveNext(out ReadOnlySpan<byte> key)
        {
            if (_currentSample >= _samples)
                goto Failure;

            // While we still have samples to use, we will travel the tree in random locations and returning
            // all the keys for each branch node that we encounter on the way down. We are just returning a
            // single key from the leaf nodes because they tend to cluster too much and bias the sample. 

            ref var cState = ref _cursor;
            ref var state = ref cState._stk[cState._pos];

            Span<byte> keySpan = Span<byte>.Empty;

            // While we haven't seen this page, we can process it. 
            while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
            {
                // If we have seen this page before, we just continue down the path. 
                if (_pagesSeen.Contains(state.Page.PageNumber))
                {
                    var next = GetValue(ref state, _generator.Next(state.Header->NumberOfEntries));
                    _tree.PushPage(next, ref cState);

                    state = ref cState._stk[cState._pos];
                }
                else
                {
                    // We havent seen it, so we are going to be processing it. 

                    // If the current index is already out of bounds, we are going to be marking this page as seen and move
                    // to the next random page down the path.
                    if (_currentIdx >= state.Header->NumberOfEntries)
                    {
                        _pagesSeen.Add(state.Page.PageNumber);
                        _currentIdx = 0;

                        // Push the next random page.
                        var next = GetValue(ref state, _generator.Next(state.Header->NumberOfEntries));
                        _tree.PushPage(next, ref cState);
                        state = ref cState._stk[cState._pos];
                    }
                    else
                    {
                        GetEntry(_tree, state.Page, state.EntriesOffsets[_currentIdx], out keySpan, out var value);
                        _currentIdx++;
                        goto Success;
                    }
                }
            }

            // If the current page is a leaf we are going to retrieve a single random key and setup everything
            // to start the random process of keys generation from the root again on the next call. 
            if (state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
            {
                int randomEntry = _generator.Next(state.Header->NumberOfEntries);
                GetEntry(_tree, state.Page, state.EntriesOffsets[randomEntry], out keySpan, out var value);

                // Move the cursor to the root.                 
                _cursor._pos = -1;
                _cursor._len = 0;
                _tree.PushPage(_tree._state.RootPage, ref _cursor);
                _currentIdx = 0;
            }

            Debug.Assert(keySpan.Length != 0, "If no key has been retrieved, there is an error in the algorithm.");

            Success:
            key = keySpan;
            _currentSample++;
            return true;

            Failure:
            key = Span<byte>.Empty;
            return false;
        }

        public void Reset()
        {
            _cursor._pos = -1;
            _cursor._len = 0;
            _tree.PushPage(_tree._state.RootPage, ref _cursor);

            _currentSample = 0;
            _currentIdx = 0;
        }
    }

    public bool ShouldImproveDictionary()
    {
        if (_llt.Flags != TransactionFlags.ReadWrite)
            return false;

        if (_state.NextTrainAt > _state.NumberOfEntries)
            return false;

        return true;
    }

    public bool TryImproveDictionary<TKeyScanner, TKeyVerifier>( TKeyScanner trainer, TKeyVerifier tester, bool force = false )
        where TKeyScanner : struct, IReadOnlySpanEnumerator
        where TKeyVerifier : struct, IReadOnlySpanEnumerator
    {
        Debug.Assert(_llt.Flags == TransactionFlags.ReadWrite);

        if (force == false && !ShouldImproveDictionary())
            return false;

        // We will try to improve the dictionary by scanning the whole tree over a maximum budget.    
        // We will do this by randomly selecting a page and then randomly selecting a key from that page.
        var currentDictionary = GetEncodingDictionary(_state.TreeDictionaryId);
        var newDictionary = PersistentDictionary.CreateIfBetter(_llt, trainer, tester, currentDictionary);

        // If the new dictionary is actually better, then update the current dictionary at the tree level.    
        if (currentDictionary != newDictionary)
            _state.TreeDictionaryId = newDictionary.PageNumber;

        // We will update the number of entries regardless if we updated the current dictionary or not. 
        _state.NextTrainAt = (long)(Math.Max(_state.NextTrainAt, _state.NumberOfEntries) * 1.5);
        return true;
    }


    /// <summary>
    /// We will try to improve the dictionary by scanning the whole tree and using the data. This may be costly and alternative and probably
    /// less effective solutions may need to be developed for when this method becomes prohibitive. 
    /// </summary>    
    public bool TryImproveDictionaryByFullScanning(bool force = false)
    {
        return TryImproveDictionary(new FullDictionaryKeyScanner(this), new FullDictionaryKeyScanner(this), force);
    }

    /// <summary>
    /// We will try to improve the dictionary by sampling from the tree. This may be less costly but at the same time
    /// it could be less effective. 
    /// </summary>
    public bool TryImproveDictionaryByRandomlyScanning(long samples, int seed = 0, bool force = false)
    {
        return TryImproveDictionary(
            new RandomBranchDictionaryKeyScanner(this, samples, seed), 
            new RandomBranchDictionaryKeyScanner(this, samples, seed), 
            force);
    }
}
