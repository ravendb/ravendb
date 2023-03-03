using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
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
        Span<byte> prevKey = Span<byte>.Empty;
        while (it.MoveNext(out Span<byte> key, out var v))
        {
            if (prevKey.SequenceCompareTo(key) > 0)
            {
                throw new InvalidDataException("The items in the compact tree are not sorted!");
            }
            prevKey = key;
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
                while (state.Header->PageFlags.HasFlag(CompactPageFlags.Branch));
            }            
        }
    }

    private void VerifyNode(ref CursorState current)
    {
        var dictionary = GetEncodingDictionary(current.Header->DictionaryId);
        if (current.Header->NumberOfEntries == 0)
            return;

        _ = GetEncodedEntry(current.Page, current.EntriesOffsets[0], out var lastEncodedKey, out var lastEncodedKeyLengthInBits, out var l);

        Span<byte> lastDecodedKey = new byte[dictionary.GetMaxDecodingBytes(lastEncodedKey.Length)];

        if (lastEncodedKey.Length != 0)
        {
            dictionary.Decode(lastEncodedKeyLengthInBits, lastEncodedKey, ref lastDecodedKey);
        }

        for (int i = 1; i < current.Header->NumberOfEntries; i++)
        {
            GetEncodedEntry(current.Page, current.EntriesOffsets[i], out var encodedKey, out var encodeKeyLengthInBits, out l);
            if (encodedKey.Length <= 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Encoded key is corrupted.");
            if (lastEncodedKey.SequenceCompareTo(encodedKey) >= 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Last encoded key does not follow lexicographically.");

            Span<byte> decodedKey = new byte[dictionary.GetMaxDecodingBytes(encodedKey.Length)];
            dictionary.Decode(encodeKeyLengthInBits, encodedKey, ref decodedKey);

            Span<byte> reencodedKey = new byte[dictionary.GetMaxEncodingBytes(decodedKey.Length)];
            dictionary.Encode(decodedKey, ref reencodedKey, out var reencodedKeyLengthInBits);

            Span<byte> decodedKey1 = new byte[dictionary.GetMaxDecodingBytes(reencodedKey.Length)];
            dictionary.Decode(reencodedKeyLengthInBits, reencodedKey, ref decodedKey1);

            if (decodedKey1.SequenceCompareTo(decodedKey) != 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Decoded key is not equal to the previous decoded key");

            // Console.WriteLine($"{Encoding.UTF8.GetString(lastDecodedKey)} - {Encoding.UTF8.GetString(decodedKey)}");

            if (lastDecodedKey.SequenceCompareTo(decodedKey) > 0 || lastEncodedKey.SequenceCompareTo(encodedKey) > 0)
            {
                Console.WriteLine($"{Encoding.UTF8.GetString(lastDecodedKey)} - {Encoding.UTF8.GetString(decodedKey)}");

                decodedKey = new byte[dictionary.GetMaxDecodingBytes(encodedKey.Length)];
                dictionary.Decode(encodeKeyLengthInBits, encodedKey, ref decodedKey);

                dictionary.Decode(lastEncodedKeyLengthInBits, lastEncodedKey, ref lastDecodedKey);
                VoronUnrecoverableErrorException.Raise(_llt, "Last encoded key does not follow lexicographically.");
            }

            lastEncodedKey = encodedKey;
            lastDecodedKey = decodedKey;
        }
    }
}
