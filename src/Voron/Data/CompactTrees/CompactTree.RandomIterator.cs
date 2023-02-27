using System;
using System.Runtime.CompilerServices;

namespace Voron.Data.CompactTrees
{
    partial class CompactTree
    {
        public unsafe struct RandomIterator
        {
            private readonly int _samples;
            private readonly Random _generator;
            private readonly CompactTree _tree;
            private int _currentSample;
            private IteratorCursorState _cursor;

            public RandomIterator(CompactTree tree, int samples, int seed = 0)
            {
                _tree = tree;
                _samples = samples;
                _currentSample = 0;
                _generator = (seed == 0) ? new() : new(seed);
                _cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };
            }

            public void Reset()
            {
                _cursor._pos = -1;
                _cursor._len = 0;
                _currentSample = 0;
            }

            public bool MoveNext(out ReadOnlySpan<byte> key, out long value)
            {
                if (_currentSample >= _samples)
                    goto Failure;

                _cursor._pos = -1;
                _cursor._len = 0;
                _tree.PushPage(_tree._state.RootPage, ref _cursor);

                ref var cState = ref _cursor;
                ref var state = ref cState._stk[cState._pos];
                if (state.Header->NumberOfEntries == 0)
                    goto Failure;

                int randomEntry;
                while (!state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                {
                    randomEntry = _generator.Next(state.Header->NumberOfEntries);

                    var next = GetValue(ref state, randomEntry);
                    _tree.PushPage(next, ref cState);

                    state = ref cState._stk[cState._pos];
                }

                randomEntry = _generator.Next(state.Header->NumberOfEntries);
                if (GetEntry(_tree, state.Page, state.EntriesOffsetsPtr[randomEntry], out var keyScope, out value) == false)
                    goto Failure;

                _currentSample++;
                key = keyScope.Key.Decoded();
                return true;


                Failure:
                key = Span<byte>.Empty;
                Unsafe.SkipInit(out value);
                return false;
            }
        }

        public RandomIterator RandomIterate(int samples, int seed = 0)
        {
            return new RandomIterator(this, samples, seed);
        }

    }
}
