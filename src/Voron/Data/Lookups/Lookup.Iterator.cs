using System;
using System.Diagnostics;

namespace Voron.Data.Lookups
{
    public interface ILookupIterator
    {
        bool IsForward { get { return false; } }
        void Init<T>(T parent);
        void Reset();
        int Fill(Span<long> results, long lastId = long.MaxValue, bool includeMax = true);
        bool Skip(long count);
        bool MoveNext(out long value);
        bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue);
        
        /// <summary>
        /// If an element equal to the key exists, the iterator is positioned at that element.
        /// 
        /// If an equal element doesn't exist:
        /// - ForwardIterator - positions at the smallest element greater than the key.
        /// - BackwardIterator - positions at the biggest element smaller than the key.
        /// 
        /// If succeeding element doesn't exist, positions iterator (LastSearchPosition) at:
        /// - ForwardIterator - position n, where n is the number of elements (effectively moving one position after actual data)
        /// - BackwardIterator - position n - 1
        /// </summary>
        /// <param name="key">Lookup key</param>
        /// <typeparam name="TLookupKey">Type of lookup key used for seek.</typeparam>
        void Seek<TLookupKey>(TLookupKey key);
    }

    unsafe partial class Lookup<TLookupKey>
    {
     
        public struct ForwardIterator : ILookupIterator
        {
            private Lookup<TLookupKey> _tree;
            private IteratorCursorState _cursor;
            private bool _isFinished;

            public ForwardIterator()
            {
                _tree = null;
                _cursor = new IteratorCursorState { _pos = -1 };
                _isFinished = false;
            }

            public bool IsForward => true;

            public void Init<T>(T tree)
            {
                if (typeof(T) != typeof(Lookup<TLookupKey>))
                {
                    throw new NotSupportedException(typeof(T).FullName);
                }
                _tree = (Lookup<TLookupKey>)(object)tree;
                _cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };
            }
            
            public void Seek<T>(T key)
            {  
                if (typeof(T) != typeof(TLookupKey))
                {
                    throw new NotSupportedException(typeof(T).FullName);
                }

                var lookupKey = (TLookupKey)(object)key;
                _tree.FindPageFor(ref lookupKey, ref _cursor);

                ref var state = ref _cursor._stk[_cursor._pos];
                if (state.LastSearchPosition < 0)
                    state.LastSearchPosition = ~state.LastSearchPosition;
            }

            public bool Skip(long count)
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                int i = 0;
                while (i < count)
                {
                    // TODO: When this works, just jump over the NumberOfEntries.
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        i++;
                        state.LastSearchPosition++;
                    }
                    else
                    {
                        if (_tree.GoToNextPage(ref _cursor) == false)
                        {
                            i++;
                        }

                        state = ref _cursor._stk[_cursor._pos];
                    }
                }

                return true;
            }
            
            public void Reset()
            {
                _isFinished = false;
                _cursor._len = 0;
                _cursor._pos = -1;
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

            public int FillKeys(Span<long> results)
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
                            results[i] = GetKeyData(ref state, state.LastSearchPosition++);
                        }
                        return read;
                    }
                    if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        return 0;
                    }
                    state = ref _cursor._stk[_cursor._pos];
                }
            }

            public int Fill(Span<long> results, long lastId = long.MaxValue, bool includeMax = true)
            {
                if (_cursor._pos < 0 || _isFinished)
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
                            
                            if (results[i] == lastId)
                            {
                                _isFinished = true;
                                return includeMax ? i + 1 : i;
                            }
                        }
                        return read;
                    }
                    if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        return 0;
                    }

                    state = ref _cursor._stk[_cursor._pos];
                }
            }
            
            /// <summary>
            /// Returns currently pointed value and moves to the next element in the tree.
            /// </summary>
            /// <param name="value">Current (before MoveNext) value.</param>
            /// <returns>Indicates if current exists.</returns>
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

                    state = ref _cursor._stk[_cursor._pos];
                }
            }

            public bool MoveNext<T>(out T key, out long value, out bool hasPreviousValue)
            {
                if (typeof(T) != typeof(TLookupKey))
                {
                    throw new NotSupportedException(typeof(T).FullName);
                }
                
                if (_cursor._pos < 0)
                {
                    key = default;
                    value = default;
                    hasPreviousValue = false;
                    return false;
                }

                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        GetKeyAndValue(ref  state, state.LastSearchPosition, out var keyData, out value);
                        key = (T)(object)TLookupKey.FromLong<TLookupKey>(keyData);
                        state.LastSearchPosition++;
                        hasPreviousValue = HasPreviousValue();
                        return true;
                    }
                    if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        key = default;
                        value = default;
                        hasPreviousValue = false;
                        return false;
                    }

                    state = ref _cursor._stk[_cursor._pos];
                }
            }

            private readonly bool HasPreviousValue()
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                if (state.LastSearchPosition > 1)
                {
                    return true;
                }

                // need to check if we have parent page with prior values
                for (int i = _cursor._pos - 2; i >= 0; i--)
                {
                    if (_cursor._stk[i].LastSearchPosition > 0)
                        return true;
                }

                return false;
            }
        }
    
        public struct BackwardIterator : ILookupIterator
        {
            private Lookup<TLookupKey> _tree;
            private IteratorCursorState _cursor;
            private bool _isFinished;
            public bool IsForward => false;

            public void Init<T>(T tree)
            {
                if (typeof(T) != typeof(Lookup<TLookupKey>))
                {
                    throw new NotSupportedException(typeof(T).FullName);
                }
                _tree = (Lookup<TLookupKey>)(object)tree;
                _cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };
                _isFinished = false;
            }

            public void Seek<T>(T key)
            {
                if (typeof(T) != typeof(TLookupKey))
                {
                    throw new NotSupportedException(typeof(T).FullName);
                }

                var lookupKey = (TLookupKey)(object)key;
                _tree.FindPageFor(ref lookupKey, ref _cursor);

                ref var state = ref _cursor._stk[_cursor._pos];
                
                if (state.LastSearchPosition >= 0)
                    return;

                state.LastSearchPosition = ~state.LastSearchPosition - 1;
            }
            
            
            public bool Skip(long count)
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                int i = 0;
                while (i < count)
                {
                    // TODO: When this works, just jump over the NumberOfEntries.
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition > 0) // same page
                    {
                        i++;
                        state.LastSearchPosition--;
                    }
                    else
                    {
                        if (_tree.GoToPreviousPage(ref _cursor) == false)
                        {
                            i++;
                        }

                        state = ref _cursor._stk[_cursor._pos];
                    }
                }

                return true;
            }

            public void Reset()
            {
                _cursor._len = 0;
                _cursor._pos = -1;
                _tree.PushPage(_tree._state.RootPage, ref _cursor);
                _isFinished = false;
                
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
            
            public int Fill(Span<long> results, long lastTermId = long.MaxValue, bool includeMax = true)
            {
                if (_cursor._pos < 0 || _isFinished)
                    return 0;
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition >= 0)
                    {
                        int read = 0;
                        while (read < results.Length && state.LastSearchPosition >= 0)
                        {
                            int curPos = state.LastSearchPosition--;
                            results[read++] = GetValue(ref state, curPos);

                            if (results[read - 1] == lastTermId)
                            {
                                _isFinished = true;
                                return includeMax ? read : read - 1;
                            }

                        }
                        return read;
                    }
                    if (_tree.GoToPreviousPage(ref _cursor) == false)
                    {
                        return 0;
                    }
                    state = ref _cursor._stk[_cursor._pos];
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
                    state = ref _cursor._stk[_cursor._pos];
                }
            }

            public bool MoveNext<T>(out T key, out long value, out bool hasPreviousValue)
            {
                if (typeof(T) != typeof(TLookupKey))
                {
                    throw new NotSupportedException(typeof(T).FullName);
                }
                
                if (_cursor._pos < 0)
                {
                    value = default;
                    key = default;
                    hasPreviousValue = false;
                    return false;
                }
                ref var state = ref _cursor._stk[_cursor._pos];
                
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
                    if (state.LastSearchPosition >= 0) // same page
                    {
                        GetKeyAndValue(ref  state, state.LastSearchPosition, out var keyData, out value);
                        key = (T)(object)TLookupKey.FromLong<TLookupKey>(keyData);

                        state.LastSearchPosition--;
                        hasPreviousValue = HasPreviousValue();
                        return true;
                    }
                    if (_tree.GoToPreviousPage(ref _cursor) == false)
                    {
                        key = default;
                        value = default;
                        hasPreviousValue = false;
                        return false;
                    }
                    state = ref _cursor._stk[_cursor._pos];
                }
            }
            
            
            private bool HasPreviousValue()
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                if (state.LastSearchPosition + 1< state.Header->NumberOfEntries)
                {
                    return true;
                }

                for (int i = _cursor._pos - 2; i >= 0; i--)
                {
                    ref var cur = ref _cursor._stk[i];
                    if (cur.LastSearchPosition + 1 < cur.Header->NumberOfEntries)
                        return true;
                }

                return false;
            }
        }

        public TDirection Iterate<TDirection>()
            where TDirection: struct, ILookupIterator
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
                Debug.Assert(state.Header->PageFlags.HasFlag(LookupPageFlags.Leaf));
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
