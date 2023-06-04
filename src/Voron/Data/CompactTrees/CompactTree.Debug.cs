using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Microsoft.VisualBasic;
using Sparrow;
using Voron.Exceptions;

namespace Voron.Data.CompactTrees;

unsafe partial class CompactTree
{
    private static class CompactTreeDumper
    {
        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteCommit(CompactTree tree)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree.Name.ToString());
            writer.WriteLine("###");
#endif
        }

        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteAddition(CompactTree tree, ReadOnlySpan<byte> key, long value)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree.Name.ToString());
            writer.WriteLine($"+{Encodings.Utf8.GetString(key)}|{value}");
#endif
        }

        [Conditional("ENABLE_COMPACT_DUMPER")]
        public static void WriteRemoval(CompactTree tree, ReadOnlySpan<byte> key)
        {
#if ENABLE_COMPACT_DUMPER
            using var writer = File.AppendText(tree.Name.ToString());
            writer.WriteLine($"-{Encodings.Utf8.GetString(key)}");
#endif
        }
    }


    public void VerifyOrderOfElements()
    {
        var it = Iterate();
        it.Reset();

        var prevKeyStorage = new byte[4096];
        Span<byte> prevKey = prevKeyStorage.AsSpan();
        while (it.MoveNext(out var scope, out var v))
        {
            var key = scope.Key.Decoded();
            if (prevKey.SequenceCompareTo(key) > 0)
            {
                throw new InvalidDataException("The items in the compact tree are not sorted!");
            }

            // We copy the current key to the storage and update.
            prevKey = prevKeyStorage.AsSpan();
            key.CopyTo(prevKey);
            prevKey = prevKey.Slice(0, key.Length);

            scope.Dispose();
        }
    }
    
    public void Verify()
    {
        IteratorCursorState cursor = new() { _stk = new CursorState[8], _pos = -1, _len = 0 };

        PushPage(_state.RootPage, ref cursor);
        VerifyNode(ref cursor._stk[0]); // Verify the root

        ref var state = ref cursor._stk[cursor._pos];

        while (!state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
        {
            var next = GetValue(ref state, 0);
            PushPage(next, ref cursor);

            state = ref cursor._stk[cursor._pos];
            VerifyNode(ref state); // We check the integrity of the current leaf.
        }

        while (true)
        {
            if (!state.Header->PageFlags.HasFlag(CompactPageFlags.Leaf))
                VoronUnrecoverableErrorException.Raise(_llt, "The current node is not a leaf, nodes are not correctly linked.");

            while (true)
            {                
                PopPage(ref cursor); // go to parent
                if (cursor._pos < 0)
                    return;               

                state = ref cursor._stk[cursor._pos];
                if (!state.Header->PageFlags.HasFlag(CompactPageFlags.Branch))
                    VoronUnrecoverableErrorException.Raise(_llt, "The current node is not a branch, nodes are not correctly linked.");

                if (++state.LastSearchPosition >= state.Header->NumberOfEntries)
                    continue; // go up
                
                do
                {
                    var next = GetValue(ref state, state.LastSearchPosition);
                    PushPage(next, ref cursor);
                    
                    state = ref cursor._stk[cursor._pos];
                    VerifyNode(ref state); // We check the integrity of the current node.                    
                }
                while (state.Header->IsBranch);
            }            
        }
    }

    private void VerifyNode(ref CursorState current)
    {
        var dictionary = _llt.GetEncodingDictionary(_state.TreeDictionaryId);
        if (current.Header->NumberOfEntries == 0)
            return;

        var lastEncodedKey = GetEncodedKeySpan(ref current, 0, out var lastEncodedKeyLengthInBits, out var l);
        
        Span<byte> lastDecodedKeyBuffer = stackalloc byte[4096];
        Span<byte> lastDecodedKey = lastDecodedKeyBuffer;

        if (lastEncodedKey.Length != 0)
        {
            dictionary.Decode(lastEncodedKeyLengthInBits, lastEncodedKey, ref lastDecodedKey);
        }

        Span<byte> decodedKeyBuffer = stackalloc byte[4096];
        Span<byte> decodedKeyBuffer2 = stackalloc byte[4096];
        Span<byte> reencodeKeyBuffer = stackalloc byte[4096];
        for (int i = 1; i < current.Header->NumberOfEntries; i++)
        {
            var encodedKey = GetEncodedKeySpan(ref current, i, out var encodeKeyLengthInBits, out l);
            if (encodedKey.Length <= 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Encoded key is corrupted.");
            if (lastEncodedKey.SequenceCompareTo(encodedKey) >= 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Last encoded key does not follow lexicographically.");
            if (dictionary.GetMaxDecodingBytes(encodedKey.Length) > decodedKeyBuffer.Length)
                throw new InvalidOperationException("Decoded key size ("+encodedKey.Length+") is too big to verify: " + dictionary.GetMaxDecodingBytes(encodedKey.Length));
            
            Span<byte> decodedKey = decodedKeyBuffer;
            decodedKey.Clear();
            dictionary.Decode(encodeKeyLengthInBits, encodedKey, ref decodedKey);
            if (dictionary.GetMaxEncodingBytes(encodedKey.Length) > decodedKeyBuffer.Length)
                throw new InvalidOperationException("Encoded key size ("+encodedKey.Length+") is too big to verify: " + dictionary.GetMaxEncodingBytes(encodedKey.Length));

            reencodeKeyBuffer.Clear();
            Span<byte> reencodedKey = reencodeKeyBuffer; 
            dictionary.Encode(decodedKey, ref reencodedKey, out var reencodedKeyLengthInBits);
            
            if (dictionary.GetMaxDecodingBytes(reencodedKey.Length) > decodedKeyBuffer2.Length)
                throw new InvalidOperationException("Rencode key size ("+reencodedKey.Length+") is too big to verify: " + dictionary.GetMaxDecodingBytes(reencodedKey.Length));

            decodedKeyBuffer2.Clear();
            Span<byte> decodedKey1 = decodedKeyBuffer2;
            dictionary.Decode(reencodedKeyLengthInBits, reencodedKey, ref decodedKey1);

            if (decodedKey1.SequenceCompareTo(decodedKey) != 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Decoded key is not equal to the previous decoded key");

            if (lastDecodedKey.SequenceCompareTo(decodedKey) > 0 || lastEncodedKey.SequenceCompareTo(encodedKey) > 0)
            {
                decodedKey = new byte[dictionary.GetMaxDecodingBytes(encodedKey.Length)];
                dictionary.Decode(encodeKeyLengthInBits, encodedKey, ref decodedKey);

                dictionary.Decode(lastEncodedKeyLengthInBits, lastEncodedKey, ref lastDecodedKey);
                VoronUnrecoverableErrorException.Raise(_llt, "Last encoded key does not follow lexicographically.");
            }

            lastEncodedKey = encodedKey;
            lastDecodedKey = lastDecodedKeyBuffer[..decodedKey.Length];
            decodedKey.CopyTo(lastDecodedKey);
        }
    }
}
