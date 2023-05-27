using System;
using System.Data;
using System.Diagnostics;
using Voron.Data.CompactTrees;

namespace Voron.Data.Lookups
{
    unsafe partial class Lookup<TKey>
    {
        public struct ForwardIterator : ITreeIterator
        {
            private Lookup<TKey> _tree;
            private IteratorCursorState _cursor;

            public void Init<T>(T tree)
            {
                _tree = (Lookup<TKey>)(object)tree;
                _cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };
            }

            public void Seek(TKey key)
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

                while (state.Header->IsBranch)
                {
                    var next = GetValue(ref state, 0);
                    _tree.PushPage(next, ref cState);

                    state = ref cState._stk[cState._pos];
                }
            }

            public int Fill(Span<long> results)
            {
                if (_cursor._pos < 0)
                    return 0;
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries)
                    {
                        var read = Math.Min(results.Length, state.Header->NumberOfEntries - state.LastSearchPosition);
                        for (int i = 0; i < read; i++)
                        {
                            results[i] = GetValue(ref state, state.LastSearchPosition++);
                        }
                        return read;
                    }
                    if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        return 0;
                    }
                }
            }
            
            public bool MoveNext(out long value)
            {
                if (_cursor._pos < 0)
                {
                    value = default;
                    return false;
                }

                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        value = GetValue(ref state, state.LastSearchPosition);
                        state.LastSearchPosition++;
                        return true;
                    }
                    if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        value = default;
                        return false;
                    }
                }
            }

            public bool MoveNext(out TKey key, out long value)
            {
                if (_cursor._pos < 0)
                {
                    key = default;
                    value = default;
                    return false;
                }

                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        GetKeyAndValue(ref  state, state.LastSearchPosition, out key, out value);
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
        }
    
        public struct BackwardIterator : ITreeIterator
        {
            private Lookup<TKey> _tree;
            private IteratorCursorState _cursor;

            public void Init<T>(T tree)
            {
                _tree = (Lookup<TKey>)(object)tree;
                _cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };
            }

            public void Seek(TKey key)
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

                while (state.Header->IsBranch)
                {
                    var lastItem = state.Header->NumberOfEntries - 1;
                    state.LastSearchPosition = lastItem;

                    var next = GetValue(ref state, lastItem);
                    _tree.PushPage(next, ref cState);

                    state = ref cState._stk[cState._pos];
                }

                state.LastSearchPosition = state.Header->NumberOfEntries - 1;
            }
            
            public int Fill(Span<long> results)
            {
                if (_cursor._pos < 0)
                    return 0;
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition >= 0)
                    {
                        int read = 0;
                        while(read < results.Length && state.LastSearchPosition >= 0)
                        {
                            int curPos = state.LastSearchPosition--;
                            results[read++] = GetValue(ref state, curPos);
                        }
                        return read;
                    }
                    if (_tree.GoToPreviousPage(ref _cursor) == false)
                    {
                        return 0;
                    }
                }
            }


            public bool MoveNext(out long value)
            {
                if (_cursor._pos < 0)
                {
                    value = default;
                    return false;
                }
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition >= 0) // same page
                    {
                        value = GetValue(ref  state, state.LastSearchPosition);
                        state.LastSearchPosition--;
                        return true;
                    }
                    if (_tree.GoToPreviousPage(ref _cursor) == false)
                    {
                        value = default;
                        return false;
                    }
                }
            }

            public bool MoveNext(out TKey key, out long value)
            {
                if (_cursor._pos < 0)
                {
                    value = default;
                    key = default;
                    return false;
                }
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition >= 0) // same page
                    {
                        GetKeyAndValue(ref  state, state.LastSearchPosition, out key, out value);

                        state.LastSearchPosition--;
                        return true;
                    }
                    if (_tree.GoToPreviousPage(ref _cursor) == false)
                    {
                        key = default;
                        value = default;
                        return false;
                    }
                }
            }
        }

        public TDirection Iterate<TDirection>()
            where TDirection:struct, ITreeIterator
        {
            var it = new TDirection();
            it.Init(this);
            return it;
        }

        public ForwardIterator Iterate()
        {
            return Iterate<ForwardIterator>();
        } 
        
        private bool GoToNextPage(ref IteratorCursorState cstate)
        {
            while (true)
            {
                PopPage(ref cstate); // go to parent
                if (cstate._pos < 0)
                    return false;

                ref var state = ref cstate._stk[cstate._pos];
                Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Branch));
                if (++state.LastSearchPosition >= state.Header->NumberOfEntries)
                    continue; // go up
                do
                {
                    var next = GetValue(ref state, state.LastSearchPosition);
                    PushPage(next, ref cstate);
                    state = ref cstate._stk[cstate._pos];
                } 
                while (state.Header->IsBranch);
                return true;
            }
        }
        
        private bool GoToPreviousPage(ref IteratorCursorState cstate)
        {
            while (true)
            {
                PopPage(ref cstate); // go to parent
                if (cstate._pos < 0)
                    return false;

                ref var state = ref cstate._stk[cstate._pos];
                Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Branch));
                if (--state.LastSearchPosition < 0)
                    continue; // go up
                do
                {
                    var next = GetValue(ref state, state.LastSearchPosition);
                    PushPage(next, ref cstate);
                    state = ref cstate._stk[cstate._pos]; 
                    state.LastSearchPosition = state.Header->NumberOfEntries - 1;
                }
                while (state.Header->IsBranch);

                state.LastSearchPosition = state.Header->NumberOfEntries - 1;
                return true;
            }
        }
    }
}
