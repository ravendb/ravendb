using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using Voron.Data.BTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.CompactTrees;

/// <summary>
/// The compact tree is conceptually a sorted_map[string,long]. It is built on top of 
/// lookup, which is a sorted_map[long,long].
/// 
/// We support map[string,long] using *external* storage of the keys, holding on
/// only to the location of the external key storage inside the tree itself.
/// 
/// A term may appear on multiple pages at the same time (if it is used as the split key in the structure
/// That means that we have may have multiple references to it.
///
/// The CompactTree doesn't have any storage behavior and is behaving just like a lookup. However, it *is*
/// managing the associated terms.  
/// </summary>
public sealed partial class CompactTree : IPrepareForCommit
{
    internal readonly Lookup<CompactKeyLookup> _inner;
    public Slice Name => _inner.Name;

    public CompactTree(Lookup<CompactKeyLookup> inner)
    {
        _inner = inner;
    }

    public unsafe struct CompactKeyLookup : ILookupKey
    {
        public CompactKey Key;
        public long ContainerId;

        public CompactKeyLookup(CompactKey key)
        {
            Key = key;
            ContainerId = -1;
        }

        public CompactKeyLookup(long containerId)
        {
            ContainerId = containerId;
            Key = null;
        }

        public void Reset()
        {
            ContainerId = -1;
        }

        public long ToLong()
        {
            return ContainerId;
        }

        public static T FromLong<T>(long l)
        {
            if (typeof(T) != typeof(CompactKeyLookup))
            {
                throw new NotSupportedException(typeof(T).FullName);
            }

            return (T)(object)new CompactKeyLookup(l);
        }

        public static long MinValue => 0;

        public CompactKey GetKey<T>(Lookup<T> parent) where T : struct, ILookupKey
        {
            if (Key != null)
                return Key;
            var llt = parent.Llt;

            byte* keyPtr;
            int keyLenInBits;
            if (ContainerId == 0)
            {
                keyPtr = null;
                keyLenInBits = 0;
            }
            else
            {
                GetEncodedKey(llt, ContainerId, out keyLenInBits, out keyPtr);
            }
            
            Key = llt.AcquireCompactKey();
            Key.Initialize(llt);
            Key.Set(keyLenInBits, keyPtr, parent.State.DictionaryId);
            return Key;
        }
        
        public void FillKey<T>(CompactKey key, Lookup<T> parent) where T : struct, ILookupKey
        {
            var llt = parent.Llt;

            byte* keyPtr;
            int keyLenInBits;
            if (ContainerId == 0)
            {
                keyPtr = null;
                keyLenInBits = 0;
            }
            else
            {
                GetEncodedKey(llt, ContainerId, out keyLenInBits, out keyPtr);
            }
            
            key.Set(keyLenInBits, keyPtr, parent.State.DictionaryId);
        }

        public void Init<T>(Lookup<T> parent) where T : struct, ILookupKey
        {
            GetKey(parent);
        }

        public int CompareTo<T>(Lookup<T> parent, long currentKeyId) where T : struct, ILookupKey
        {
            var llt = parent.Llt;

            byte* keyPtr;
            int keyLengthInBits;
            if (currentKeyId == 0)
            {
                keyPtr = null;
                keyLengthInBits = 0;
            }
            else
            {
                GetEncodedKey(llt, currentKeyId, out keyLengthInBits, out keyPtr);
            }

            var k = GetKey(parent);
            if (ReferenceEquals(k, CompactKey.NullInstance))
                return -1; // null is smallest
            
            var match = k.CompareEncodedWithCurrent(keyPtr, keyLengthInBits);
            if (match == 0)
            {
                ContainerId = currentKeyId;
            }

            return match;
        }

        private static void GetEncodedKey(LowLevelTransaction llt, long l, out int encodedKeyLengthInBits, out byte* encodedKeyPtr) 
        {
            Container.Get(llt, l, out var keyItem);
            int remainderInBits = *keyItem.Address >> 4;
            var encodedKeyLen = keyItem.Length - 1;
            encodedKeyLengthInBits = encodedKeyLen * 8 - remainderInBits;
            encodedKeyPtr = keyItem.Address + 1;
        }

        public void OnNewKeyAddition<T>(Lookup<T> parent) where T : struct, ILookupKey
        {
            CreateTermInContainer(parent);

            IncrementTermReferenceCount(parent.Llt, ContainerId);
        }

        public void CreateTermInContainer<T>(Lookup<T> parent) where T : struct, ILookupKey
        {
            if (ContainerId == -1) // need to save this in the external terms container
            {
                var encodedKey = Key.EncodedWithCurrent(out int encodedKeyLengthInBits);
                ContainerId = WriteTermToContainer(encodedKey, encodedKeyLengthInBits, ref parent.State, parent.Llt);
            }
        }

        public void OnKeyRemoval<T>(Lookup<T> parent) where T : struct, ILookupKey
        {
            if (ContainerId == 0)
                return; // the empty key is not stored and cannot be referenced
            DecrementTermReferenceCount(parent.Llt, ContainerId, ref parent.State);
        }

        public string ToString<T>(Lookup<T> parent) where T : struct, ILookupKey
        {
            return GetKey(parent).ToString();
        }

        public override string ToString()
        {
            return Key?.ToString() ?? "ContainerId: " + ContainerId;
        }

        private void DecrementTermReferenceCount(LowLevelTransaction llt, long keyContainerId, ref LookupState state)
        {
            var term = Container.GetMutable(llt, keyContainerId);
            int termRefCount = term[0] & 0xF;
            if (termRefCount == 0)
                throw new VoronErrorException("A term exists without any references? That should be impossible");
            if (termRefCount == 1) // no more references, can delete
            {
                Container.Delete(llt, state.TermsContainerId, keyContainerId);
                return;
            }

            term[0] = (byte)((term[0] & 0xF0) | (termRefCount - 1));
        }


        private void IncrementTermReferenceCount(LowLevelTransaction llt, long keyContainerId)
        {
            var term = Container.GetMutable(llt, keyContainerId);
            int termRefCount = term[0] & 0xF;
            // A term usage count means that it is used by multiple pages at the same time. That can happen only if a leaf & branches are using
            // the same term as the separator. However, that has natural limits. As the term can only be in one path through the tree, the tree height
            // is a natural limit. Compact tree at maximum storage will take ~17 bytes, which means at *least* 425 items, which means that that the 
            // height of the tree if we store 2^64 items it in would be 8. So even if we assume bad insert factor, which will increase the height of the
            // tree, having a limit of 15 is reasonable
            if (termRefCount == 15)
                throw new VoronErrorException($"A term is used at max(tree-height), but we have term: {keyContainerId} used: {termRefCount}. This is a bug");

            term[0] = (byte)((term[0] & 0xF0) | termRefCount + 1);
        }


        private static long WriteTermToContainer(ReadOnlySpan<byte> encodedKey, int encodedKeyLengthInBits, ref LookupState state, LowLevelTransaction llt)
        {
            // the term in the container is:  [ metadata -1 byte ] [ term bytes ]
            // the metadata is composed of two nibbles - the first says the *remainder* of bits in the last byte in the term
            // the second nibble is the reference count
            var id = Container.Allocate(llt, state.TermsContainerId, encodedKey.Length + 1, state.RootPage, out var allocated);
            encodedKey.CopyTo(allocated[1..]);
            var remainderBits = encodedKey.Length * 8 - encodedKeyLengthInBits;
            Debug.Assert(remainderBits is >= 0 and < 8);
            allocated[0] = (byte)(remainderBits << 4); // ref count of 0, will be incremented shortly
            return id;
        }


        [Pure]
        public int CompareTo<T>(T l) where T : ILookupKey
        {
            if (typeof(T) != typeof(CompactKeyLookup))
            {
                throw new NotSupportedException(typeof(T).FullName);
            }

            var other = (CompactKeyLookup)(object)l;
            return Key.Compare(other.Key);
        }

        [Pure]
        public bool IsEqual<T>(T k) where T : ILookupKey
        {
            if (typeof(T) != typeof(CompactKeyLookup))
            {
                throw new NotSupportedException(typeof(T).FullName);
            }

            var other = (CompactKeyLookup)(object)k;
            return ContainerId == other.ContainerId;
        }
    }

    public long DictionaryId => _inner.State.DictionaryId;
    public long NumberOfEntries => _inner.State.NumberOfEntries;
    public long RootPage => _inner.State.RootPage;
    public long BranchPages => _inner.State.BranchPages;
    public long LeafPages => _inner.State.LeafPages;

    public void Add(string key, long value)
    {
        using var _ = Slice.From(_inner.Llt.Allocator, key, out var slice);
        using var scope = new CompactKeyCacheScope(_inner.Llt, slice.AsReadOnlySpan(), _inner.State.DictionaryId);
        Add(scope.Key, value);
    }

    public void Add(ReadOnlySpan<byte> key, long value)
    {
        using var scope = new CompactKeyCacheScope(_inner.Llt, key, _inner.State.DictionaryId);
        Add(scope.Key, value);
    }

    public long AddAfterTryGetNext(ref CompactKeyLookup lookup, long value)
    {
        CompactTreeDumper.WriteAddition(this, ref lookup, value);

        _inner.AddAfterTryGetNext(ref lookup, value);
        return lookup.ContainerId;
    }
    
    public void SetAfterTryGetNext(ref CompactKeyLookup lookup, long value)
    {
        _inner.SetAfterTryGetNext(ref lookup, value);
    }


    public long Add(CompactKey key, long value)
    {
        key.ChangeDictionary(_inner.State.DictionaryId);
        AssertValueAndKeySize(key, value);

        var lookup = new CompactKeyLookup(key);
        CompactTreeDumper.WriteAddition(this, ref lookup, value);
        _inner.Add(ref lookup, value);

        return lookup.ContainerId;
    }

    public void PrepareForCommit()
    {
        _inner.PrepareForCommit();
        CompactTreeDumper.WriteCommit(this);
    }

    private void AssertValueAndKeySize(CompactKey key, long value)
    {
        if (value < 0)
            throw new ArgumentOutOfRangeException(nameof(value), value, "Only positive values are allowed");
        if (key.MaxLength > Constants.CompactTree.MaximumKeySize)
            throw new ArgumentOutOfRangeException(nameof(key), Encoding.UTF8.GetString(key.Decoded()),
                $"key must be less than {Constants.CompactTree.MaximumKeySize} bytes in size");
    }


    public static unsafe CompactTree InternalCreate(Tree parent, Slice name)
    {
        Lookup<CompactKeyLookup> inner;
        var llt = parent.Llt;
        var existing = parent.Read(name);
        if (existing == null)
        {
            if (llt.Flags != TransactionFlags.ReadWrite)
                return null;

            if ((parent.ReadHeader().Flags & TreeFlags.CompactTrees) != TreeFlags.CompactTrees)
            {
                // We will modify the parent tree state because there are Compact Trees in it.
                ref var parentHeader = ref parent.ModifyHeader();
                parentHeader.Flags |= TreeFlags.CompactTrees;
            }

            long dictionaryId;

            // This will be created a single time and stored in the root page.
            using var scoped = Slice.From(llt.Allocator, PersistentDictionary.DictionaryKey, out var defaultKey);
            var existingDictionary = llt.RootObjects.DirectRead(defaultKey);
            if (existingDictionary == null)
            {
                dictionaryId = PersistentDictionary.CreateDefault(llt);

                using var scope = llt.RootObjects.DirectAdd(defaultKey, sizeof(PersistentDictionaryRootHeader), out var ptr);
                *(PersistentDictionaryRootHeader*)ptr = new PersistentDictionaryRootHeader()
                {
                    RootObjectType = RootObjectType.PersistentDictionary,
                    PageNumber = dictionaryId
                };
            }
            else
            {
                dictionaryId = ((PersistentDictionaryRootHeader*)existingDictionary)->PageNumber;
            }

            long containerId = Container.Create(llt);
            inner = Lookup<CompactKeyLookup>.InternalCreate(parent, name, dictionaryId, containerId);
        }
        else
        {
            inner = Lookup<CompactKeyLookup>.InternalCreate(parent, name);
        }

        return new CompactTree(inner);
    }

    public static bool HasDictionary(LowLevelTransaction llt)
    {
        using var scoped = Slice.From(llt.Allocator, PersistentDictionary.DictionaryKey, out var dictionarySlice);
        var existingDictionary = llt.RootObjects.Read(dictionarySlice);
        return existingDictionary != null;
    }
    
    public static unsafe long GetDictionaryId(LowLevelTransaction llt)
    {
        using var scoped = Slice.From(llt.Allocator, PersistentDictionary.DictionaryKey, out var dictionarySlice);
        var read = llt.RootObjects.Read(dictionarySlice);
        if (read != null)
        {
            return ((PersistentDictionaryRootHeader*)read.Reader.Base)->PageNumber;
        }
        return -1;
    }

    public bool TryGetValue(string key, out long value)
    {
        using var _ = Slice.From(_inner.Llt.Allocator, key, out var slice);
        var span = slice.AsReadOnlySpan();

        return TryGetValue(span, out value);
    }

    public bool TryGetValue(ReadOnlySpan<byte> key, out long value)
    {
        using var scope = new CompactKeyCacheScope(_inner.Llt, key, _inner.State.DictionaryId);
        return _inner.TryGetValue(new CompactKeyLookup(scope.Key), out value);
    }

    public bool TryGetValue(CompactKey key, out long value)
    {
        key.ChangeDictionary(_inner.State.DictionaryId);
        return _inner.TryGetValue(new CompactKeyLookup(key), out value);
    }

    public bool TryGetTermContainerId(CompactKey key, out long value)
    {
        key.ChangeDictionary(_inner.State.DictionaryId);
        return _inner.TryGetTermContainerId(new CompactKeyLookup(key), out value);
    }

    public bool TryGetValue(CompactKey key, out long termContainerId, out long value)
    {
        key.ChangeDictionary(_inner.State.DictionaryId);
        CompactKeyLookup compactKeyLookup = new(key);
        var result = _inner.TryGetValue(ref compactKeyLookup, out value);
        termContainerId = compactKeyLookup.ContainerId;
        return result;
    }
    
    
    public bool TryRemove(string key, out long oldValue)
    {
        using var _ = Slice.From(_inner.Llt.Allocator, key, out var slice);
        var span = slice.AsReadOnlySpan();
        return TryRemove(span, out oldValue);
    }

    public bool TryRemove(Slice key, out long oldValue)
    {
        return TryRemove(key.AsReadOnlySpan(), out oldValue);
    }

    public bool TryRemove(ReadOnlySpan<byte> key, out long oldValue)
    {
        var compactKey = new CompactKey();
        compactKey.Initialize(_inner.Llt);
        compactKey.Set(key);
        compactKey.ChangeDictionary(_inner.State.DictionaryId);
        return TryRemove(new CompactKeyLookup(compactKey), out oldValue);
    }
    
    public bool TryRemove(CompactKeyLookup key, out long oldValue)
    {
        var result = _inner.TryRemove(key, out oldValue);
        CompactTreeDumper.WriteRemoval(this, ref key, oldValue);
        return result;
    }

    public bool TryRemoveExistingValue(ref CompactKeyLookup key, out long oldValue)
    {
        var result = _inner.TryRemoveExistingValue(ref key, out oldValue);
        CompactTreeDumper.WriteRemoval(this, ref key, oldValue);
        return result;
    }

    public void InitializeStateForTryGetNextValue()
    {
        _inner.InitializeCursorState();
    }

    public bool TryGetNextValue(CompactKey key, out long termContainerId, out long value,out CompactKeyLookup lookup)
    {
        key.ChangeDictionary(DictionaryId);
        key.EncodedWithCurrent(out _);
        lookup = new CompactKeyLookup(key);
        var result = _inner.TryGetNextValue(ref lookup, out value);
        termContainerId = lookup.ContainerId;
        return result;
    }
    
    
    public void BulkUpdateSet(ref CompactKeyLookup key, long value, long pageNum, int offset, ref int adjustment)
    {
        _inner.BulkUpdateSet(ref key, value, pageNum, offset, ref adjustment);
        CompactTreeDumper.WriteBulkSet(this, ref key, value);
    }

    public Lookup<CompactKeyLookup>.TreeStructureChanged CheckTreeStructureChanges()
    {
        return _inner.CheckTreeStructureChanges();
    }

    public bool BulkUpdateRemove(ref CompactKeyLookup key, long pageNum, int offset, ref int adjustment, out long oldValue)
    {
        var result = _inner.BulkUpdateRemove(ref key, pageNum, offset, ref adjustment, out oldValue);
        CompactTreeDumper.WriteBulkRemoval(this, ref key, oldValue);
        return result;
    }
    
    public int BulkUpdateStart(Span<CompactKeyLookup> keys, Span<long> values, Span<int> offsets, out long pageNum)
    {
        #if DEBUG
        for (int i = 0; i < keys.Length; i++)
        {
            Debug.Assert(keys[i].ContainerId == -1, "keys[i].ContainerId == -1");
        }
        #endif
        var read = _inner.BulkUpdateStart(keys, values, offsets, out pageNum);
        for (int i = 0; i < read; i++)
        {
            if (keys[i].ContainerId == -1)
            {
                keys[i].CreateTermInContainer(_inner);
            }
        }

        return read;
    }

    public List<long> AllPages() => _inner.AllPages();

    public List<(string, long)> AllEntriesIn(long p) => 
        _inner.AllEntriesIn(p)
            .Select(x=>(x.Item1.GetKey(_inner).ToString(), x.Item2))
            .ToList();

    public void VerifyStructure()
    {
        _inner.VerifyStructure();
    }
}
