// -----------------------------------------------------------------------
//  <copyright file="SmallStringCompression.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.InteropServices;
using System.Text;
using Sparrow;

namespace Raven.Server.Json
{
    /// <summary>
    /// Based on https://github.com/antirez/smaz
    /// Full explanation is here:
    /// https://ayende.com/blog/172865/reverse-engineering-the-smaz-compression-library?key=cd1aaeff5ab64f1ebcf6e5556f9a7803
    /// </summary>
    public unsafe class SmallStringCompression
    {
        // ReSharper disable once RedundantExplicitArraySize
        private readonly string[] TermsTable = new string[246]
        {
            " ", "the", "e", "t", "a", "of", "o", "and", "i", "n", "s", "e ", "r", " th",
            " t", "in", "he", "th", "h", "he ", "to", "\r\n", "l", "s ", "d", " a", "an",
            "er", "c", " o", "d ", "on", " of", "re", "of ", "t ", ", ", "is", "u", "at",
            "   ", "n ", "or", "which", "f", "m", "as", "it", "that", "\n", "was", "en",
            "  ", " w", "es", " an", " i", "f ", "g", "p", "nd", " s", "nd ", "ed ",
            "w", "ed", "http://", "https://", "for", "te", "ing", "y ", "The", " c", "ti", "r ", "his",
            "st", " in", "ar", "nt", ",", " to", "y", "ng", " h", "with", "le", "al", "to ",
            "b", "ou", "be", "were", " b", "se", "o ", "ent", "ha", "ng ", "their", "\"",
            "hi", "from", " f", "in ", "de", "ion", "me", "v", ".", "ve", "all", "re ",
            "ri", "ro", "is ", "co", "f t", "are", "ea", ". ", "her", " m", "er ", " p",
            "es ", "by", "they", "di", "ra", "ic", "not", "s, ", "d t", "at ", "ce", "la",
            "h ", "ne", "as ", "tio", "on ", "n t", "io", "we", " a ", "om", ", a", "s o",
            "ur", "li", "ll", "ch", "had", "this", "e t", "g ", " wh", "ere",
            " co", "e o", "a ", "us", " d", "ss", " be", " e","@",
            "s a", "ma", "one", "t t", "or ", "but", "el", "so", "l ", "e s", "s,", "no",
            "ter", " wa", "iv", "ho", "e a", " r", "hat", "s t", "ns", "ch ", "wh", "tr",
            "ut", "/", "have", "ly ", "ta", " ha", " on", "tha", "-", " l", "ati", "en ",
            "pe", " re", "there", "ass", "si", " fo", "wa", "ec", "our", "who", "its", "z",
            "fo", "rs", "ot", "un", "im", "th ", "nc", "ate", "ver", "ad",
            " we", "ly", "ee", " n", "id", " cl", "ac", "il", "rt", " wi",
            "e, ", " it", "whi", " ma", "ge", "x", "e c", "men", ".com"
        };

        private readonly byte*[] _termsTableBytes;

        private readonly byte*[][] _hashTable;

        private readonly int _maxTermSize;
        private readonly int _maxVerbatimLen;

        public static SmallStringCompression Instance = new SmallStringCompression(); 

        private SmallStringCompression()
        {
            if (TermsTable.Length + 8 > byte.MaxValue)
                throw new InvalidOperationException("Too many terms defined");

            _termsTableBytes = new byte*[TermsTable.Length];
            _maxVerbatimLen = byte.MaxValue - TermsTable.Length;
            _hashTable = new byte*[byte.MaxValue][];
            for (int i = 0; i < TermsTable.Length; i++)
            {
                var byteCount = Encoding.UTF8.GetByteCount(TermsTable[i]);
                if (byteCount > byte.MaxValue)
                    throw new InvalidOperationException("Term " + TermsTable[i] + " is too big");
                var ptr = (byte*)Marshal.AllocHGlobal(byteCount + 2);
                _termsTableBytes[i] = ptr;
                ptr[0] = (byte)byteCount;
                fixed (char* pChars = TermsTable[i])
                {
                    var bytes = Encoding.UTF8.GetBytes(pChars, TermsTable[i].Length, ptr + 1, byteCount);
                    if (bytes != byteCount)
                        throw new InvalidOperationException("Bug, UTF8 encoding mismatch for GetByteCount and GetBytes for " + TermsTable[i]);
                }
                ptr[byteCount+1] = (byte)i;

                _maxTermSize = Math.Max(_maxTermSize, byteCount);

                AddToHash(ptr, byteCount);
            }
            var empty = new byte*[0];

            for (int i = 0; i < _hashTable.Length; i++)
            {
                if (_hashTable[i] == null)
                    _hashTable[i] = empty;
            }
        }

        private void AddToHash(byte* ptr, int byteCount)
        {
            int h = ptr[1] << 3;
            AddToHash(h, ptr);
            if (byteCount == 1)
                return;
            h += ptr[2];
            AddToHash(h, ptr);
            if (byteCount == 2)
                return;
            h ^= ptr[3];
            AddToHash(h, ptr);
        }

        private void AddToHash(int hash, byte* buffer)
        {
            var index = hash % _hashTable.Length;
            if (_hashTable[index] == null)
            {
                _hashTable[index] = new[] { buffer };
                return;
            }
            var newBuffer = new byte*[_hashTable[index].Length + 1];
            for (int i = 0; i < _hashTable[index].Length; i++)
            {
                newBuffer[i] = _hashTable[index][i];
            }
            newBuffer[newBuffer.Length - 1] = buffer;
            _hashTable[index] = newBuffer;
        }

        public int Decompress(byte* input, int inputLen, byte* output, int outputLen)
        {
            var outPos = 0;
            for (int i = 0; i < inputLen; i++)
            {
                var slot = input[i];
                if (slot >= TermsTable.Length)
                {
                    // verbatim entry
                    var len = slot - TermsTable.Length;
                    if (outPos+len > outputLen)
                        return outputLen + 1;
                    Memory.Copy(output, input + i + 1, len);
                    outPos += len;
                    output += len;
                    i += len;
                }
                else
                {
                    var len = _termsTableBytes[slot][0];
                    if (outPos + len > outputLen)
                        return outputLen + 1;
                    Memory.Copy(output, _termsTableBytes[slot] + 1, len);
                    output += len;
                    outPos += len;
                }
            }
            return outPos;
        }

        public int Compress(byte* input, int inputLen, byte* output, int outputLen)
        {
            var outPos = 0;
            var verbatimStart = 0;
            var verbatimLength = 0;
            var verbatimPos = 0;
            for (int i = 0; i < inputLen; i++)
            {
                int h1, h2 = 0, h3 = 0;
                h1 = input[i] << 3;
                if (i + 1 < inputLen)
                    h2 = h1 + input[i + 1];
                if (i + 2 < inputLen)
                    h3 = h2 ^ input[i + 2];

                int size = _maxTermSize;
                if (i + size >= inputLen)
                    size = inputLen - i;
                var foundMatch = false;
                for (; size > 0 && foundMatch == false; size--)
                {
                    byte*[] slot;
                    switch (size)
                    {
                        case 1: slot = _hashTable[h1 % _hashTable.Length]; break;
                        case 2: slot = _hashTable[h2 % _hashTable.Length]; break;
                        default: slot = _hashTable[h3 % _hashTable.Length]; break;
                    }
                    if (slot == null)
                        continue;
                    for (int j = 0; j < slot.Length; j++)
                    {
                        var termLegnth = slot[j][0];
                        if (termLegnth != size ||
                            Memory.Compare(input + i, slot[j] + 1, size) != 0)
                        {
                            continue;
                        }
                        verbatimPos = outPos;
                        if (outPos + verbatimLength + 1 > outputLen)
                            return outputLen + 1;
                        outPos += verbatimLength;
                        output[outPos++] = slot[j][termLegnth + 1]; // get the index to write there
                        verbatimStart = i + termLegnth;
                        i += termLegnth - 1; // skip the length we just compressed
                        foundMatch = true;
                        break;
                    }
                }
                if (foundMatch || i == inputLen - 1)
                {
                    while (verbatimLength > 0)
                    {
                        var len = Math.Min(_maxVerbatimLen - 1, verbatimLength);
                        if (outPos + len > outputLen)
                            return outputLen + 1;
                        output[outPos++] = (byte)(len + TermsTable.Length);
                        Memory.Copy(output + verbatimPos, input + verbatimStart, verbatimLength);
                        verbatimStart += len;
                        verbatimLength -= len;
                        verbatimPos += len;
                    }
                }
                else
                {
                    verbatimLength++;
                }
            }
            return outPos;
        }
    }
}
