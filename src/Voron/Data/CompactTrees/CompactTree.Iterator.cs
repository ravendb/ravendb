using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server.Strings;

namespace Voron.Data.CompactTrees
{
    partial class CompactTree
    {
        // TODO: Implement this and use it instead of the tree directly. 
        public unsafe struct Iterator 
        {
            private readonly CompactTree _tree;
            private IteratorCursorState _cursor;

            public Iterator(CompactTree tree)
            {
                _tree = tree;
                _cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };
            }

            public void Seek(string key)
            {
                using var _ = Slice.From(_tree._llt.Allocator, key, out var slice);
                Seek(slice);
            }

            public void Seek(ReadOnlySpan<byte> key)
            {
                _tree.FindPageFor(key, ref _cursor);
                ref var state = ref _cursor._stk[_cursor._pos];
                if (state.LastSearchPosition < 0)
                    state.LastSearchPosition = ~state.LastSearchPosition;
            }

            public void Reset()
            {
                _tree.PushPage(_tree._state.RootPage, ref _cursor);

                ref var cState = ref _cursor;
                ref var state = ref cState._stk[cState._pos];

                while (!state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                {
                    var next = GetValue(ref state, 0);
                    _tree.PushPage(next, ref cState);

                    state = ref cState._stk[cState._pos];
                }
            }

            public bool MoveNext(out Slice key, out long value)
            {
                var next = MoveNext(out Span<byte> keySpan, out value);
                if (next)
                {    
                    Slice.From(_tree._llt.Allocator, keySpan, out key);
                }
                else
                {
                    key = default(Slice);
                }

                return next;
            }

            public bool MoveNext(out Span<byte> key, out long value)
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        GetEntry(_tree, state.Page, state.EntriesOffsets[state.LastSearchPosition], out key, out value);
                        state.LastSearchPosition++;
                        return true;
                    }
                    if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        key = default;
                        value = default;
                        return false;
                    }
                }
            }

            public bool MovePrev(out Span<byte> key, out long value)
            {
                throw new NotImplementedException();
            }

            public bool MovePrev(out Slice key, out long value)
            {
                throw new NotImplementedException();
            }

            public bool Skip(long count)
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                int i = 0;
                while (i < count)
                {
                    // TODO: When this works, just jump over the NumberOfEntries.
                    Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        i++;
                        state.LastSearchPosition++;
                    }
                    else if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        i++;
                    }
                }

                return true;

            }
        }

        public unsafe struct FuzzyIterator
        {
            private Slice _baseKey;
            private readonly float _minScore;
            private readonly CompactTree _tree;
            private IteratorCursorState _cursor;

            public FuzzyIterator(CompactTree tree, Slice baseKey, float maxDistance)
            {
                _baseKey = baseKey;
                _minScore = maxDistance;
                _tree = tree;
                _cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };
            }

            public void Seek(ReadOnlySpan<byte> key)
            {
                _tree.FuzzyFindPageFor(key, ref _cursor);
                
                ref var state = ref _cursor._stk[_cursor._pos];
                if (state.LastSearchPosition < 0)
                    state.LastSearchPosition = ~state.LastSearchPosition;
            }

            public void Reset()
            {
                Seek(_baseKey);                
            }

            public bool MoveNext(out Slice key, out long value, out float score)
            {
                var next = MoveNext(out Span<byte> keySpan, out value, out score);
                if (next)
                {
                    Slice.From(_tree._llt.Allocator, keySpan, out key);
                }
                else
                {
                    key = default(Slice);
                }

                return next;
            }

            public bool MoveNext(out Span<byte> key, out long value, out float score)
            {
                LevenshteinDistance distance = default;

                byte firstLetter = _baseKey[0];

                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        GetEntry(_tree, state.Page, state.EntriesOffsets[state.LastSearchPosition], out key, out value);
                        state.LastSearchPosition++;                     

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

                    if (_tree.GoToNextPage(ref _cursor) == false)
                        goto IsDone;
                }

            IsDone:
                key = default;
                value = default;
                score = 0;
                return false;
            }

            public bool MovePrev(out Span<byte> key, out long value, out float score)
            {
                throw new NotImplementedException();
            }

            public bool MovePrev(out Slice key, out long value, out float score)
            {
                throw new NotImplementedException();
            }

            public bool Skip(long count)
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                int i = 0;
                while (i < count)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        i++;
                        state.LastSearchPosition++;
                    }
                    else if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        i++;
                    }
                }

                return true;
            }
        }

        public FuzzyIterator FuzzyIterate(Slice fuzzyKey, float distance)
        {
            return new FuzzyIterator(this, fuzzyKey, distance);
        }

        public Iterator Iterate()
        {
            return new Iterator(this);
        }
    }
}
