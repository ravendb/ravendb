using System;
using System.Data;
using System.Diagnostics;

namespace Voron.Data.CompactTrees
{
    partial class CompactTree
    {
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

            public void Seek(CompactKey key)
            {
                key.ChangeDictionary(_tree.State.TreeDictionaryId);
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
                if (_cursor._pos < 0)
                {
                    scope = default;
                    value = default;
                    return false;
                }
                ref var state = ref _cursor._stk[_cursor._pos];
                while (true)
                {
                    Debug.Assert(state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf));
                    if (state.LastSearchPosition < state.Header->NumberOfEntries) // same page
                    {
                        if (GetEntry(_tree, ref  state, state.LastSearchPosition, out scope, out value) == false)
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
        }

        public Iterator Iterate()
        {
            return new Iterator(this);
        }        
    }
}
