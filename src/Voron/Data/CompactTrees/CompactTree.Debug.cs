using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow;
using Voron.Exceptions;

namespace Voron.Data.CompactTrees;

unsafe partial class CompactTree
{

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
        var dictionary = _dictionaries[current.Header->DictionaryId];

        int len = GetEncodedEntry(current.Page, current.EntriesOffsets[0], out var lastEncodedKey, out var l);

        Span<byte> lastDecodedKey = new byte[dictionary.GetMaxDecodingBytes(lastEncodedKey)];
        dictionary.Decode(lastEncodedKey, ref lastDecodedKey);

        for (int i = 1; i < current.Header->NumberOfEntries; i++)
        {
            GetEncodedEntry(current.Page, current.EntriesOffsets[i], out var encodedKey, out l);
            if (encodedKey.Length <= 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Encoded key is corrupted.");
            if (lastEncodedKey.SequenceCompareTo(encodedKey) >= 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Last encoded key does not follow lexicographically.");

            Span<byte> decodedKey = new byte[dictionary.GetMaxDecodingBytes(encodedKey)];
            dictionary.Decode(encodedKey, ref decodedKey);

            Span<byte> reencodedKey = new byte[dictionary.GetMaxEncodingBytes(decodedKey)];
            dictionary.Encode(decodedKey, ref reencodedKey);

            Span<byte> decodedKey1 = new byte[dictionary.GetMaxDecodingBytes(reencodedKey)];
            dictionary.Decode(encodedKey, ref decodedKey1);

            if (decodedKey1.SequenceCompareTo(decodedKey) != 0)
                VoronUnrecoverableErrorException.Raise(_llt, "Decoded key is not equal to the previous decoded key");

            // Console.WriteLine($"{Encoding.UTF8.GetString(lastDecodedKey)} - {Encoding.UTF8.GetString(decodedKey)}");

            if (lastDecodedKey.SequenceCompareTo(decodedKey) > 0 || lastEncodedKey.SequenceCompareTo(encodedKey) > 0)
            {
                Console.WriteLine($"{Encoding.UTF8.GetString(lastDecodedKey)} - {Encoding.UTF8.GetString(decodedKey)}");

                decodedKey = new byte[dictionary.GetMaxDecodingBytes(encodedKey)];
                dictionary.Decode(encodedKey, ref decodedKey);

                dictionary.Decode(lastEncodedKey, ref lastDecodedKey);
                VoronUnrecoverableErrorException.Raise(_llt, "Last encoded key does not follow lexicographically.");
            }

            lastEncodedKey = encodedKey;
            lastDecodedKey = decodedKey;
        }
    }
}
