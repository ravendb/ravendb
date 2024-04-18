using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Server;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.CompactTrees;

// The idea behind the introduction of the CompactKey is to serve as an abstraction that allow to cache the
// encoding of the same key under different dictionaries in order to save the cost of encoding/decoding 
// when the same key is reused on multiple operations in the same transaction.
public unsafe class CompactKey : IDisposable
{
    public readonly LowLevelTransaction Owner;

    private ByteString _storage;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _storageScope;

    // The storage data will be used in an arena fashion. If there is no enough, we just create a bigger one and
    // copy the content back. 
    private byte* _currentPtr;
    private byte* _currentEndPtr;

    // The decoded key pointer points toward the actual decoded key (if available). If not we will take the current
    // dictionary and just decode it into the storage. 
    private int _decodedKeyIdx;
    private int _currentKeyIdx;

    private const int MappingTableSize = 64;
    private const int MappingTableMask = MappingTableSize - 1;

    private readonly long* _keyMappingCache;
    private readonly long* _keyMappingCacheIndex;
    private int _lastKeyMappingItem;

    public int MaxLength { get; private set; }

    public long Dictionary { get; private set; }

    private const int Invalid = -1;

    public CompactKey(LowLevelTransaction tx)
    {
        Owner = tx;
        Dictionary = Invalid;
        _currentKeyIdx = Invalid;
        _decodedKeyIdx = Invalid;
        _lastKeyMappingItem = Invalid;

        int allocationSize = 2 * Constants.CompactTree.MaximumKeySize + 2 * MappingTableSize * sizeof(long);

        _storageScope = Owner.Allocator.Allocate(allocationSize, out _storage);
        _currentPtr = _storage.Ptr;
        _currentEndPtr = _currentPtr + Constants.CompactTree.MaximumKeySize * 2;

        _keyMappingCache = (long*)_currentEndPtr;
        _keyMappingCacheIndex = (long*)(_currentEndPtr + MappingTableSize * sizeof(long));

        MaxLength = 0;
    }

    public bool IsValid => Dictionary > 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> EncodedWithCurrent(out int lengthInBits)
    {
        Debug.Assert(IsValid, "Cannot get an encoded key without a current dictionary");

        var keyPtr = EncodedWithPtr(Dictionary, out lengthInBits);
        int keyLength = Bits.ToBytes(lengthInBits);
        return new ReadOnlySpan<byte>(keyPtr, keyLength);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* EncodedWithPtr(long dictionaryId, out int lengthInBits)
    {
        [SkipLocalsInit]
        byte* EncodeFromDecodedForm()
        {
            Debug.Assert(IsValid, "At this stage we either created the key using an unencoded version. Current dictionary cannot be invalid.");

            // We acquire the decoded form. This will lazily evaluate if needed. 
            ReadOnlySpan<byte> decodedKey = Decoded();

            // IMPORTANT: Pointers are potentially invalidated by the grow storage call but not the indexes.

            // We look for an appropriate place to put this key
            int buckedIdx = SelectBucketForWrite();
            _keyMappingCache[buckedIdx] = dictionaryId;
            _keyMappingCacheIndex[buckedIdx] = (int)(_currentPtr - _storage.Ptr);

            var dictionary = Owner.GetEncodingDictionary(dictionaryId);
            int maxSize = dictionary.GetMaxEncodingBytes(decodedKey.Length) + 4;

            int currentSize = (int)(_currentEndPtr - _currentPtr);
            if (maxSize > currentSize)
                UnlikelyGrowStorage(currentSize + maxSize);

            var encodedStartPtr = _currentPtr;

            var encodedKey = new Span<byte>(encodedStartPtr + sizeof(int), maxSize);
            dictionary.Encode(decodedKey, ref encodedKey, out var encodedKeyLengthInBits);

            *(int*)encodedStartPtr = encodedKeyLengthInBits;
            _currentPtr += encodedKey.Length + sizeof(int);
            MaxLength = Math.Max(encodedKey.Length, MaxLength);

            return encodedStartPtr;
        }

        byte* start;
        if (Dictionary == dictionaryId && _currentKeyIdx != Invalid)
        {
            // This is the fast-path, we are requiring the usage of a dictionary that happens to be the current one. 
            start = _storage.Ptr + _currentKeyIdx;
        }
        else
        {
            int bucketIdx = SelectBucketForRead(dictionaryId);
            if (bucketIdx == Invalid)
            {
                start = EncodeFromDecodedForm();
                bucketIdx = _lastKeyMappingItem;

                // IMPORTANT: Pointers are potentially invalidated by the grow storage call at EncodeFromDecodedForm, be careful here. 
            }
            else
            {
                start = _storage.Ptr + _keyMappingCacheIndex[bucketIdx];
            }

            // Because we are decoding for the current dictionary, we will update the lazy index to avoid searching again next time. 
            if (Dictionary == dictionaryId)
                _currentKeyIdx = (int)_keyMappingCacheIndex[bucketIdx];
        }

        lengthInBits = *(int*)start;
        return start + sizeof(int);
    }

    [SkipLocalsInit]
    private void DecodeFromEncodedForm()
    {
        Debug.Assert(IsValid, "At this stage we either created the key using an unencoded version OR we have already pushed 1 encoded key. Current dictionary cannot be invalid.");

        long currentDictionary = Dictionary;
        int currentKeyIdx = _currentKeyIdx;
        if (currentKeyIdx == Invalid && _keyMappingCache[0] != 0)
        {
            // We don't have any decoded version, so we pick the first one and do it. 
            currentDictionary = _keyMappingCache[0];
            currentKeyIdx = (int)_keyMappingCacheIndex[0];
        }

        Debug.Assert(currentKeyIdx != Invalid);

        var dictionary = Owner.GetEncodingDictionary(currentDictionary);

        byte* encodedStartPtr = _storage.Ptr + currentKeyIdx;
        int encodedKeyLengthInBits = *(int*)encodedStartPtr;
        int encodedKeyLength = Bits.ToBytes(encodedKeyLengthInBits);

        int maxSize = dictionary.GetMaxDecodingBytes(encodedKeyLength) + sizeof(int);
        int currentSize = (int)(_currentEndPtr - _currentPtr);
        if (maxSize > currentSize)
        {
            // IMPORTANT: Pointers are potentially invalidated by the grow storage call but not the indexes. 
            UnlikelyGrowStorage(maxSize + currentSize);
            encodedStartPtr = _storage.Ptr + currentKeyIdx;
        }

        _decodedKeyIdx = (int)(_currentPtr - _storage.Ptr);

        var decodedKey = new Span<byte>(_currentPtr + sizeof(int), maxSize);
        dictionary.Decode(encodedKeyLengthInBits, new ReadOnlySpan<byte>(encodedStartPtr + sizeof(int), encodedKeyLength), ref decodedKey);

        *(int*)_currentPtr = decodedKey.Length;
        _currentPtr += decodedKey.Length + sizeof(int);
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
        byte* start = _storage.Ptr + _decodedKeyIdx;
        int length = *((int*)start);

        return new ReadOnlySpan<byte>(start + sizeof(int), length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* DecodedPtr(out int lengthInBytes)
    {
        if (_decodedKeyIdx == Invalid)
        {
            DecodeFromEncodedForm();
        }

        // IMPORTANT: Pointers are potentially invalidated by the grow storage call at DecodeFromEncodedForm, be careful here. 
        byte* start = _storage.Ptr + _decodedKeyIdx;
        // This is decoded therefore is bytes.
        lengthInBytes = *((int*)start);

        return start + sizeof(int);
    }

    private void UnlikelyGrowStorage(int maxSize)
    {
        int memoryUsed = (int)(_currentPtr - _storage.Ptr);

        // Request more memory, copy the content and return it.
        maxSize = Math.Max(maxSize, _storage.Length) * 2;
        var storageScope = Owner.Allocator.Allocate(maxSize, out var storage);
        Memory.Copy(storage.Ptr, _storage.Ptr, memoryUsed);

        _storageScope.Dispose();

        // Update the new references.
        _storage = storage;
        _storageScope = storageScope;

        // This procedure will invalidate any pointer beyond this point. 
        _currentPtr = _storage.Ptr + memoryUsed;
        _currentEndPtr = _currentPtr + _storage.Length;
    }

    public void Set(CompactKey key)
    {
        Dictionary = key.Dictionary;
        _currentKeyIdx = key._currentKeyIdx;
        _decodedKeyIdx = key._decodedKeyIdx;
        _lastKeyMappingItem = key._lastKeyMappingItem;

        var originalSize = (int)(key._currentPtr - key._storage.Ptr);
        if (originalSize > _storage.Length)
            UnlikelyGrowStorage(originalSize);

        // Copy the key mapping and content.
        int lastElementIdx = Math.Min(_lastKeyMappingItem, MappingTableSize - 1);
        if (lastElementIdx >= 0)
        {
            var srcDictionary = key._keyMappingCache;
            var destDictionary = _keyMappingCache;
            var srcIndex = key._keyMappingCacheIndex;
            var destIndex = _keyMappingCacheIndex;

            int currentElementIdx = 0;

            // PERF: Since we are avoiding the cost of general purpose copying, if we have the vector instruction set we should use it. 
            if (Avx.IsSupported)
            {
                // Find out the last element where a full vector can be copied.
                int lastVectorElement = (key._lastKeyMappingItem / Vector256<long>.Count) * Vector256<long>.Count;

                while (currentElementIdx < lastElementIdx)
                {
                    Avx.Store(_keyMappingCache + currentElementIdx, Avx.LoadDquVector256(key._keyMappingCache + currentElementIdx));
                    Avx.Store(_keyMappingCacheIndex + currentElementIdx, Avx.LoadDquVector256(key._keyMappingCacheIndex + currentElementIdx));
                    currentElementIdx += Vector256<long>.Count;
                }
            }

            while (currentElementIdx < lastElementIdx)
            {
                destDictionary[currentElementIdx] = srcDictionary[currentElementIdx];
                destIndex[currentElementIdx] = srcIndex[currentElementIdx];
                currentElementIdx++;
            }
        }

        // This is the operation to set an unencoded key, therefore we need to restart everything.
        _currentPtr = _storage.Ptr;
        Memory.Copy(_currentPtr, key._storage.Ptr, originalSize);
        _currentPtr += originalSize;

        MaxLength = originalSize;
    }

    public void Set(ReadOnlySpan<byte> key)
    {
        // This is the operation to set an unencoded key, therefore we need to restart everything.
        var currentPtr = _storage.Ptr;

        _lastKeyMappingItem = Invalid;

        // Since the size is big enough to store the unencoded key, we don't check the remaining size here.
        _decodedKeyIdx = (int)(currentPtr - (long)_storage.Ptr);
        _currentKeyIdx = Invalid;
        Dictionary = Invalid;

        int keyLength = key.Length;

        // We write the size and the key. 
        *((int*)currentPtr) = keyLength;
        currentPtr += sizeof(int);

        // PERF: Between pinning the pointer and just execute the Unsafe.CopyBlock unintuitively it is faster to just copy. 
        Unsafe.CopyBlock(ref Unsafe.AsRef<byte>(currentPtr), ref Unsafe.AsRef<byte>(in key[0]), (uint)keyLength);

        currentPtr += keyLength; // We update the new pointer. 
        _currentPtr = currentPtr;

        MaxLength = keyLength;
    }

    public void Set(int keyLengthInBits, ReadOnlySpan<byte> key, long dictionaryId)
    {
        _lastKeyMappingItem = Invalid;

        // This is the operation to set an unencoded key, therefore we need to restart everything.
        _currentPtr = _storage.Ptr;

        // Since the size is big enough to store twice the unencoded key, we don't check the remaining size here.
        fixed (byte* keyPtr = key)
        {
            // This is the current state after the setup with the encoded value.
            _decodedKeyIdx = Invalid;
            _currentKeyIdx = (int)(_currentPtr - (long)_storage.Ptr);
            Dictionary = dictionaryId;

            int bucketIdx = SelectBucketForWrite();
            _keyMappingCache[bucketIdx] = dictionaryId;
            _keyMappingCacheIndex[bucketIdx] = _currentKeyIdx;

            // We write the size and the key. 
            *(int*)_currentPtr = keyLengthInBits;
            _currentPtr += sizeof(int);

            int keyLength = Bits.ToBytes(keyLengthInBits);
            Memory.Copy(_currentPtr, keyPtr, keyLength);
            _currentPtr += keyLength; // We update the new pointer. 

            MaxLength = keyLength;
        }
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

    public int CompareEncodedWithCurrent(byte* nextEntryPtr, int nextEntryLength)
    {
        if (Dictionary == Invalid)
            throw new VoronErrorException("The dictionary is not set.");

        if (_currentKeyIdx == Invalid)
            return CompareEncodedWith(nextEntryPtr, nextEntryLength, Dictionary);

        // This method allows us to compare the key in it's encoded form directly using the current dictionary. 
        byte* encodedStartPtr = _storage.Ptr + _currentKeyIdx;
        int length = *((int*)encodedStartPtr);

        var result = AdvMemory.CompareInline(encodedStartPtr + sizeof(int), nextEntryPtr, Math.Min(length, nextEntryLength));
        return result == 0 ? length - nextEntryLength : result;
    }

    public int CompareEncodedWith(byte* nextEntryPtr, int nextEntryLengthInBits, long dictionaryId)
    {
        // This method allow us to compare the key in it's encoded form using an arbitrary dictionary without changing
        // the current dictionary/cached state. 
        byte* encodedStartPtr;
        int encodedLengthInBits;
        if (Dictionary == dictionaryId && _currentKeyIdx != Invalid)
        {
            Debug.Assert(_currentKeyIdx != Invalid, "The current key index is not set and it should be.");

            encodedStartPtr = _storage.Ptr + _currentKeyIdx;
            encodedLengthInBits = *((int*)encodedStartPtr);
            encodedStartPtr += sizeof(int);
        }
        else
        {
            encodedStartPtr = EncodedWithPtr(dictionaryId, out encodedLengthInBits);
            MaxLength = Bits.ToBytes(encodedLengthInBits);
        }

        int nextEntryLength = Bits.ToBytes(nextEntryLengthInBits);
        int encodedLength = Bits.ToBytes(encodedLengthInBits);
        var result = AdvMemory.CompareInline(encodedStartPtr, nextEntryPtr, Math.Min(encodedLength, nextEntryLength));
        return result == 0 ? encodedLength - nextEntryLength : result;
    }

    public void Dispose()
    {
        _storageScope.Dispose();
    }

    public int Compare(CompactKey value)
    {
        // If both are using the same dictionary, we just get the current encoded value.
        if (this.Dictionary != Invalid && this.Dictionary == value.Dictionary)
        {
            byte* valuePtr = value.EncodedWithPtr(value.Dictionary, out var valueLength);
            return CompareEncodedWithCurrent(valuePtr, valueLength);
        }

        // This is the fallback, let's hope that both have the decoded value already there to avoid
        // the decoding step. 
        var thisDecoded = this.Decoded();
        var valueDecoded = value.Decoded();
        return thisDecoded.SequenceCompareTo(valueDecoded);
    }
}
