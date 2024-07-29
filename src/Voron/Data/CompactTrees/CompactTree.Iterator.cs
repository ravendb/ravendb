using System;
using System.Runtime.CompilerServices;
using Voron.Data.Lookups;

namespace Voron.Data.CompactTrees
{
    partial class CompactTree
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Iterator<Lookup<CompactKeyLookup>.ForwardIterator> Iterate()
        {
            return Iterate<Lookup<CompactKeyLookup>.ForwardIterator>();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Iterator<TDirection> Iterate<TDirection>() where TDirection : struct, ILookupIterator
        {
            TDirection direction = _inner.Iterate<TDirection>();
            return new Iterator<TDirection>(this, direction);
        }
        
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public TDirection IterateValues<TDirection>() where TDirection : struct,  ILookupIterator
        {
            return _inner.Iterate<TDirection>();
        }

        public struct Iterator<TDirection> where TDirection : struct, ILookupIterator
        {
            private readonly CompactTree _tree;
            private TDirection _it;

            public Iterator(CompactTree tree,TDirection it)
            {
                _tree = tree;
                _it = it;
            }
            
            public void Seek(string key)
            {
                using var _ = Slice.From(_tree._inner.Llt.Allocator, key, out var slice);
                Seek(slice);
            }

            public void Seek(ReadOnlySpan<byte> key)
            {
                using var scope = new CompactKeyCacheScope(_tree._inner.Llt);
                var encodedKey = scope.Key;
                encodedKey.Set(key);
                encodedKey.ChangeDictionary(_tree._inner.State.DictionaryId);

                Seek(encodedKey);
            }

            public void Seek(CompactKey key)
            {
                key.ChangeDictionary(_tree._inner.State.DictionaryId);
                _it.Seek(new CompactKeyLookup(key));
            }

            public void Reset()
            {
                _it.Reset();
            }

            public bool Skip(long count) => _it.Skip(count);
            
            public int Fill(Span<long> matches, long lastTermId = long.MaxValue, bool includeMax = true) => _it.Fill(matches, lastTermId, includeMax);

            public bool MoveNext(out long v) => _it.MoveNext(out v);
            
            
            public bool MoveNext(out CompactKey key, out long v, out bool hasPreviousValue)
            {
                if (_it.MoveNext(out CompactKeyLookup keyData, out v, out hasPreviousValue) == false)
                {
                    key = default;
                    return false;
                }
                
                key = keyData.GetKey(_tree._inner);
                return true;
            }
            
            public unsafe bool MoveNext(CompactKey key, out long v, out bool hasPreviousValue)
            {
                
                
                if (_it.MoveNext(out CompactKeyLookup keyData, out v, out hasPreviousValue) == false)
                {
                    key = default;
                    return false;
                }
                
                keyData.FillKey(key, _tree._inner);
                return true;
            }
        }
    }
}
