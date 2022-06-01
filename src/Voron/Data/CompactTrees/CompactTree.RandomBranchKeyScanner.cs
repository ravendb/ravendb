using System;
using System.Collections.Generic;
using System.Diagnostics;
using Sparrow;

namespace Voron.Data.CompactTrees;



unsafe partial class CompactTree
{
    // The random branch scanner is trade-off between the fully sequential scanner performance and the
    // stochastic nature of the fully random scanner. 
    // It will move randomly from the top to the bottom of the tree following a random path.
    // Whatever it hits a branch page that hasnt been seen before, it will loop over those keys until there are no more keys.
    // When that happens it will deem the page as seen and added to the _pagesSeen set.
    // On the next MoveNext it will continue down the path until either it finds a new unseen branch OR it hits a leaf.
    // If the latter happens, it will just return a single sample (as in the random scanner) and reset the cursor to the root.
    public struct RandomBranchKeyScanner : IReadOnlySpanEnumerator
    {
        private readonly long _samples;        
        private readonly Random _generator;
        private readonly CompactTree _tree;
        private int _currentSample;
        private int _currentIdx;
        private IteratorCursorState _cursor;
        private HashSet<long> _pagesSeen;

        public RandomBranchKeyScanner(CompactTree tree, long samples, int? seed)
        {
            _tree = tree;
            _samples = samples;
            _currentSample = 0;
            _currentIdx = 0;
            _generator = (seed.HasValue) ? new(seed.Value) : new();
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
}
