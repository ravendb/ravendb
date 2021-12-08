using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Voron.Data.CompactTrees
{
    partial class CompactTree
    {
        public interface IIterator
        {
            void Reset();
            void Seek(ReadOnlySpan<byte> key);
            bool Skip(long count);

            bool MoveNext(out Span<byte> key, out long value);
            bool MovePrev(out Span<byte> key, out long value);
            bool MoveNext(out Slice key, out long value);
            bool MovePrev(out Slice key, out long value);
        }

        // TODO: Implement this and use it instead of the tree directly. 
        public unsafe struct Iterator : IIterator
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

        public Iterator Iterate()
        {
            return new Iterator(this);
        }
    }
}
