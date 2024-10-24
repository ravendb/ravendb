using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.CompactTrees;

// The idea behind the introduction of the CompactKey is to serve as an abstraction that allow to cache the
// encoding of the same key under different dictionaries in order to save the cost of encoding/decoding 
// when the same key is reused on multiple operations in the same transaction.
public sealed unsafe class CompactKey : IDisposable
{
    public static readonly CompactKey NullInstance = new();

    [ThreadStatic]
    private static ArrayPool<byte> StoragePool;
    [ThreadStatic]
    private static ArrayPool<long> KeyMappingPool;

    private LowLevelTransaction _owner;

    private const int MappingTableSize = 64;
    private const int MappingTableMask = MappingTableSize - 1;

    private long[] _keyMappingCache;
    private ref long KeyMappingCache(int i) => ref _keyMappingCache[i];
    private ref long KeyMappingCacheIndex(int i) => ref _keyMappingCache[MappingTableSize + i];


    private byte[] _storage;

    // The storage data will be used in an arena fashion. If there is no enough, we just create a bigger one and
    // copy the content back. 
    private int _currentIdx;

    // The decoded key pointer points toward the actual decoded key (if available). If not we will take the current
    // dictionary and just decode it into the storage. 
    private int _decodedKeyIdx;
    private int _currentKeyIdx;


    private int _lastKeyMappingItem;

    public int MaxLength;

    public long Dictionary;

    private const int Invalid = -1;

    public void Initialize(LowLevelTransaction tx)
    {
        _owner = tx;
        
        StoragePool ??= ArrayPool<byte>.Create();
        KeyMappingPool ??= ArrayPool<long>.Create();

        _storage = StoragePool.Rent(2 * Constants.CompactTree.MaximumKeySize);
        _keyMappingCache = KeyMappingPool.Rent(2 * MappingTableMask);
        
        Reuse();
    }

    public void Reuse()
    {
        Dictionary = Invalid;
        _currentKeyIdx = Invalid;
        _decodedKeyIdx = Invalid;
        _lastKeyMappingItem = Invalid;

        _currentIdx = 0;
        MaxLength = 0;

        StoragePool ??= ArrayPool<byte>.Create();
        KeyMappingPool ??= ArrayPool<long>.Create();

        _storage ??= StoragePool.Rent(2 * Constants.CompactTree.MaximumKeySize);
        _keyMappingCache ??= KeyMappingPool.Rent(2 * MappingTableMask);
    }

    public void Reset()
    {
        PortableExceptions.ThrowIf<InvalidOperationException>(
            _storage is null || _keyMappingCache is null, 
            "The key has not been initialized before calling reset.");

        _owner = null;
    }

    public bool IsValid => Dictionary > 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> EncodedWithCurrent(out int lengthInBits)
    {
        Debug.Assert(IsValid, "Cannot get an encoded key without a current dictionary");

        return EncodedWith(Dictionary, out lengthInBits);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> EncodedWith(long dictionaryId, out int lengthInBits)
    {
        [SkipLocalsInit]
        int EncodeFromDecodedForm()
        {
            Debug.Assert(IsValid, "At this stage we either created the key using an unencoded version. Current dictionary cannot be invalid.");

            // We acquire the decoded form. This will lazily evaluate if needed. 
            ReadOnlySpan<byte> decodedKey = Decoded();

            // IMPORTANT: Pointers are potentially invalidated by the grow storage call but not the indexes.

            // We look for an appropriate place to put this key
            int buckedIdx = SelectBucketForWrite();
            KeyMappingCache(buckedIdx) = dictionaryId;
            KeyMappingCacheIndex(buckedIdx) = _currentIdx;

            var dictionary = _owner.GetEncodingDictionary(dictionaryId);
            int maxSize = dictionary.GetMaxEncodingBytes(decodedKey.Length) + 4;

            int expectedSize = maxSize + _currentIdx + sizeof(int);
            if (expectedSize > _storage.Length)
                UnlikelyGrowStorage(expectedSize);

            int encodedStartIdx = _currentIdx;
            var encodedKey = _storage.AsSpan(encodedStartIdx + sizeof(int), maxSize);
            dictionary.Encode(decodedKey, ref encodedKey, out var encodedKeyLengthInBits);

            Unsafe.WriteUnaligned(ref _storage[encodedStartIdx], encodedKeyLengthInBits);
            _currentIdx += encodedKey.Length + sizeof(int);
            MaxLength = Math.Max(encodedKey.Length, MaxLength);

            return encodedStartIdx;
        }

        int startIdx;
        if (Dictionary == dictionaryId && _currentKeyIdx != Invalid)
        {
            // This is the fast-path, we are requiring the usage of a dictionary that happens to be the current one. 
            startIdx = _currentKeyIdx;
        }
        else
        {
            int bucketIdx = SelectBucketForRead(dictionaryId);
            if (bucketIdx == Invalid)
            {
                startIdx = EncodeFromDecodedForm();
                bucketIdx = _lastKeyMappingItem;

                // IMPORTANT: Pointers are potentially invalidated by the grow storage call at EncodeFromDecodedForm, be careful here. 
            }
            else
            {
                startIdx = (int)KeyMappingCacheIndex(bucketIdx);
            }

            // Because we are decoding for the current dictionary, we will update the lazy index to avoid searching again next time. 
            if (Dictionary == dictionaryId)
                _currentKeyIdx = (int)KeyMappingCacheIndex(bucketIdx);
        }


        lengthInBits = Unsafe.ReadUnaligned<int>(ref _storage[startIdx]);
        return _storage.AsSpan(startIdx + sizeof(int), Bits.ToBytes(lengthInBits));
    }

    [SkipLocalsInit]
    private void DecodeFromEncodedForm()
    {
        Debug.Assert(IsValid, "At this stage we either created the key using an unencoded version OR we have already pushed 1 encoded key. Current dictionary cannot be invalid.");

        long currentDictionary = Dictionary;
        int currentKeyIdx = _currentKeyIdx;
        if (currentKeyIdx == Invalid && KeyMappingCache(0) != 0)
        {
            // We don't have any decoded version, so we pick the first one and do it. 
            currentDictionary = KeyMappingCache(0);
            currentKeyIdx = (int)KeyMappingCacheIndex(0);
        }

        Debug.Assert(currentKeyIdx != Invalid);

        var dictionary = _owner.GetEncodingDictionary(currentDictionary);

        int encodedStartIdx = currentKeyIdx;
        int encodedKeyLengthInBits = Unsafe.ReadUnaligned<int>(ref _storage[encodedStartIdx]);
        int encodedKeyLength = Bits.ToBytes(encodedKeyLengthInBits);

        int maxSize = dictionary.GetMaxDecodingBytes(encodedKeyLength) + sizeof(int);
        int expectedSize = maxSize + sizeof(int) + _currentIdx;
        if (expectedSize > _storage.Length)
        {
            // IMPORTANT: Pointers are potentially invalidated by the grow storage call but not the indexes. 
            UnlikelyGrowStorage(expectedSize);
            encodedStartIdx = currentKeyIdx;
        }

        _decodedKeyIdx = _currentIdx;

        var decodedKey = _storage.AsSpan(_currentIdx + sizeof(int), maxSize);
        var encodedKey = _storage.AsSpan(encodedStartIdx + sizeof(int), encodedKeyLength);
        dictionary.Decode(encodedKeyLengthInBits, encodedKey, ref decodedKey);

        Unsafe.WriteUnaligned(ref _storage[_currentIdx], decodedKey.Length);
        
        _currentIdx += decodedKey.Length + sizeof(int);
        MaxLength = Math.Max(decodedKey.Length, MaxLength);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> Decoded()
    {
        if (_decodedKeyIdx == Invalid)
        {
            DecodeFromEncodedForm();
        }

        // IMPORTANT: Pointers are potentially invalidated by the grow storage call at DecodeFromEncodedForm, be careful here. 
        int length = Unsafe.ReadUnaligned<int>(ref _storage[_decodedKeyIdx]);
        return _storage.AsSpan(_decodedKeyIdx + sizeof(int), length);
    }

    private void UnlikelyGrowStorage(int maxSize)
    {
        var oldStorage = _storage;

        // Request more memory, copy the content and return it.
        maxSize = Math.Max(maxSize, oldStorage.Length) * 2;

        var storage = StoragePool.Rent(maxSize);
        _storage.AsSpan(0, _currentIdx).CopyTo(storage.AsSpan());
        
        StoragePool.Return(_storage); // Return old to pool.
        _storage = storage; // Update the new references.
    }

    public void Set(ReadOnlySpan<byte> key)
    {
        // This is the operation to set an unencoded key, therefore we need to restart everything.
        _lastKeyMappingItem = Invalid;

        // Since the size is big enough to store the unencoded key, we don't check the remaining size here.
        _decodedKeyIdx = 0;
        _currentKeyIdx = Invalid;
        Dictionary = Invalid;

        int maxLength = key.Length + sizeof(int);
        if (maxLength > _storage.Length)
            UnlikelyGrowStorage(maxLength);

        Debug.Assert(_storage.Length >= maxLength);

        // We write the size and the key. 
        Unsafe.WriteUnaligned<int>(ref _storage[0], key.Length);

        // PERF: Between pinning the pointer and just execute the Unsafe.CopyBlock unintuitively it is faster to just copy. 
        ref readonly byte kPtr = ref key[0];
        Unsafe.CopyBlock(ref _storage[sizeof(int)],  in kPtr, (uint)key.Length);

        _currentIdx = key.Length + sizeof(int);

        MaxLength = key.Length;
    }

    public void Set(int keyLengthInBits, ReadOnlySpan<byte> key, long dictionaryId)
    {
        Set(keyLengthInBits, ref MemoryMarshal.GetReference(key), dictionaryId);
    }

    public void Set(int keyLengthInBits, byte* keyPtr, long dictionaryId)
    {
        Set(keyLengthInBits, ref Unsafe.AsRef<byte>(keyPtr), dictionaryId);
    }

    public void Set(int keyLengthInBits, ref byte keyRef, long dictionaryId)
    {
        _lastKeyMappingItem = Invalid;

        // This is the operation to set an unencoded key, therefore we need to restart everything.

        // Since the size is big enough to store twice the unencoded key, we don't check the remaining size here.

        // This is the current state after the setup with the encoded value.
        _decodedKeyIdx = Invalid;
        _currentKeyIdx = 0;
        Dictionary = dictionaryId;

        int bucketIdx = SelectBucketForWrite();
        KeyMappingCache(bucketIdx) = dictionaryId;
        KeyMappingCacheIndex(bucketIdx) = _currentKeyIdx;

        int keyLength = Bits.ToBytes(keyLengthInBits);
        int maxLength = keyLength + sizeof(int);
        if (maxLength > _storage.Length)
            UnlikelyGrowStorage(maxLength);

        Debug.Assert(_storage.Length >= maxLength);

        // We write the size and the key. 
        Unsafe.WriteUnaligned(ref _storage[0], keyLengthInBits);

        // PERF: Between pinning the pointer and just execute the Unsafe.CopyBlock unintuitively it is faster to just copy. 
        Unsafe.CopyBlock(ref _storage[sizeof(int)], ref keyRef, (uint)keyLength);

        _currentIdx = keyLength + sizeof(int); // We update the new pointer. 

        MaxLength = keyLength;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int SelectBucketForRead(long dictionaryId)
    {
        // TODO: This can exploit AVX2 instructions.
        int elementIdx = Math.Min(_lastKeyMappingItem, MappingTableSize - 1);
        while (elementIdx >= 0)
        {
            long currentDictionary = _keyMappingCache[elementIdx];
            if (currentDictionary == dictionaryId)
                return elementIdx;

            elementIdx--;
        }

        return Invalid;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int SelectBucketForWrite()
    {
        _lastKeyMappingItem++;
        return _lastKeyMappingItem & MappingTableMask;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void ChangeDictionary(long dictionaryId)
    {
        Debug.Assert(dictionaryId > 0, "The dictionary id must be valid to perform a change.");

        // With this operation we can change the current dictionary which would force a search
        // in the hash structure and if the key is not found setup everything so that it gets
        // lazily reencoded on the next access.

        if (dictionaryId == Dictionary)
            return;

        Dictionary = dictionaryId;
        _currentKeyIdx = Invalid;
    }

    public int CompareEncodedWithCurrent(byte* nextEntryPtr, int nextEntryLengthInBits)
    {
        if (Dictionary == Invalid)
            throw new VoronErrorException("The dictionary is not set.");

        if (_currentKeyIdx == Invalid)
            return CompareEncodedWith(ref Unsafe.AsRef<byte>(nextEntryPtr), nextEntryLengthInBits, Dictionary);

        // This method allows us to compare the key in it's encoded form directly using the current dictionary. 
        int encodedStartIdx = _currentKeyIdx;

        int lengthInBits = Unsafe.ReadUnaligned<int>(ref _storage[encodedStartIdx]);

        var length = Bits.ToBytes(lengthInBits);
        var nextEntryLength = Bits.ToBytes(nextEntryLengthInBits);

        var result = Memory.CompareInline(ref _storage[encodedStartIdx + sizeof(int)], ref Unsafe.AsRef<byte>(nextEntryPtr), Math.Min(length, nextEntryLength));
        return result == 0 ? lengthInBits - nextEntryLengthInBits : result;
    }

    public int CompareEncodedWithCurrent(ref byte nextEntryRef, int nextEntryLengthInBits)
    {
        if (Dictionary == Invalid)
            throw new VoronErrorException("The dictionary is not set.");

        return CompareEncodedWith(ref nextEntryRef, nextEntryLengthInBits, Dictionary);
    }

    public int CompareEncodedWith(ref byte nextEntryRef, int nextEntryLengthInBits, long dictionaryId)
    {
        // This method allow us to compare the key in it's encoded form using an arbitrary dictionary without changing
        // the current dictionary/cached state. 
        ReadOnlySpan<byte> encodedKey;
        int encodedLengthInBits;
        if (Dictionary == dictionaryId && _currentKeyIdx != Invalid)
        {
            Debug.Assert(_currentKeyIdx != Invalid, "The current key index is not set and it should be.");

            encodedLengthInBits = Unsafe.ReadUnaligned<int>(ref _storage[_currentKeyIdx]);
            encodedKey = _storage.AsSpan(_currentKeyIdx + sizeof(int), Bits.ToBytes(encodedLengthInBits));
        }
        else
        {
            encodedKey = EncodedWith(dictionaryId, out encodedLengthInBits);
            MaxLength = Bits.ToBytes(encodedLengthInBits);
        }

        int nextEntryLength = Bits.ToBytes(nextEntryLengthInBits);
        int encodedLength = Bits.ToBytes(encodedLengthInBits);

        var result = Memory.CompareInline(ref MemoryMarshal.GetReference(encodedKey), ref nextEntryRef, Math.Min(encodedLength, nextEntryLength));
        return result == 0 ? encodedLength - nextEntryLength : result;
    }
    
    public void Dispose()
    {
        if (_storage is not null)
        {
            StoragePool.Return(_storage);
            _storage = null;
        }

        if (_keyMappingCache is not null)
        {
            KeyMappingPool.Return(_keyMappingCache);
            _keyMappingCache = null;
        }

        _owner = null;
    }

    public int Compare(CompactKey value)
    {
        // If both are using the same dictionary, we just get the current encoded value.
        if (Dictionary != Invalid && Dictionary == value.Dictionary)
        {
            var valueStream = value.EncodedWith(value.Dictionary, out var valueLength);
            return CompareEncodedWithCurrent(ref MemoryMarshal.GetReference(valueStream), valueLength);
        }

        // This is the fallback, let's hope that both have the decoded value already there to avoid
        // the decoding step. 
        var thisDecoded = Decoded();
        var valueDecoded = value.Decoded();
        return thisDecoded.SequenceCompareTo(valueDecoded);
    }

    public override string ToString()
    {
        if (IsValid == false)
            return "Key: [NotValid]";
        
        return Encoding.UTF8.GetString(Decoded());
    }
}
