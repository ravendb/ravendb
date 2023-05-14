using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Voron.Data.CompactTrees
{
    partial class CompactTree
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ForwardIterator Iterate()
        {
            return Iterate<ForwardIterator>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TDirection Iterate<TDirection>() where TDirection : struct, ICompactTreeIterator
        {
            var it = new TDirection();
            it.Init(this);
            return it;
        }

        public unsafe struct ForwardIterator : ICompactTreeIterator
        {
            private CompactTree _tree;
            private IteratorCursorState _cursor;

            public void Init(CompactTree tree)
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

            public void Seek(CompactKey key)
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

            public bool MoveNext(out CompactKeyCacheScope scope, out long value)
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        if (GetEntry(_tree, ref state, state.LastSearchPosition, out scope, out value) == false)
                        {
                            scope.Dispose();
                            return false;
                        }

                        state.LastSearchPosition++;
                        return true;
                    }
                    if (_tree.GoToNextPage(ref _cursor) == false)
                    {
                        scope = default;
                        value = default;
                        return false;
                    }
                }
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

        public unsafe struct BackwardIterator : ICompactTreeIterator
        {
            private CompactTree _tree;
            private IteratorCursorState _cursor;

            public void Init(CompactTree tree)
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

            public void Seek(CompactKey key)
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

            public bool MoveNext(out CompactKeyCacheScope scope, out long value)
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                    if (state.LastSearchPosition >= 0) // same page
                    {
                        GetEntry(_tree, ref state, state.LastSearchPosition, out scope, out value);

                        state.LastSearchPosition--;
                        return true;
                    }
                    if (_tree.GoToPreviousPage(ref _cursor) == false)
                    {
                        scope = default;
                        value = default;
                        return false;
                    }
                }
            }

            public bool Skip(long count)
            {
                ref var state = ref _cursor._stk[_cursor._pos];
                int i = 0;
                while (i < count)
                {
                    // TODO: When this works, just jump over the NumberOfEntries.
                    Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                    if (state.LastSearchPosition > 0) // same page
                    {
                        i++;
                        state.LastSearchPosition--;
                    }
                    else if (_tree.GoToPreviousPage(ref _cursor) == false)
                    {
                        i++;
                    }
                }

                return true;
            }
        }
    }
}
