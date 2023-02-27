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
        private readonly HashSet<long> _pagesSeen;

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
            CompactKeyCacheScope scope = default;

            if (_currentSample >= _samples)
                goto Failure;

            // While we still have samples to use, we will travel the tree in random locations and returning
            // all the keys for each branch node that we encounter on the way down. We are just returning a
            // single key from the leaf nodes because they tend to cluster too much and bias the sample. 

            ref var cState = ref _cursor;
            ref var state = ref cState._stk[cState._pos];

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
                    // We haven't seen it, so we are going to be processing it. 

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
                        if (GetEntry(_tree, state.Page, state.EntriesOffsetsPtr[_currentIdx], out scope, out _) == false)
                            goto Failure;
                        
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
                if (GetEntry(_tree, state.Page, state.EntriesOffsetsPtr[randomEntry], out scope, out _) == false)
                    goto Failure;

                // Move the cursor to the root.                 
                _cursor._pos = -1;
                _cursor._len = 0;
                _tree.PushPage(_tree._state.RootPage, ref _cursor);
                _currentIdx = 0;
            }

            Debug.Assert(scope.Key.IsValid, "If no key has been retrieved, there is an error in the algorithm.");

            // It is important to note that given we are returning a read only reference, and we cannot know what
            // the caller is gonna do, the keys must survive until struct is collected. 

            Success:
            _currentSample++;
            key = scope.Key.Decoded();
            return true;

            Failure:
            key = ReadOnlySpan<byte>.Empty;
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
