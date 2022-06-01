using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Sparrow.Server;

namespace Corax.Utils
{
    internal static class SuggestionsKeys
    {
        internal static ByteString Generate(ByteStringContext allocator, int ngramSize, ReadOnlySpan<byte> term, out int keysCount)
        {
            // This method will generate the ngram based suggestion keys. The idea behind this keys is that we could perform
            // a very efficient starts-with operation on the suggestions tree in order to find the proper documents.
            // Keys are been constructed in such a way that common ngrams gets pushed at the start of the key. 
            // The general format is: {ngram}{term} 
            // Therefore now we can go insert such a key and perform multiple calls to start-with with the different ngrams
            // to find all potential terms that share that ngram.

            if (term.Length <= ngramSize)
            {
                // If the term is smaller than the ngram size, it will be stored in its entirely. 
                allocator.From(term, out var single);
                keysCount = 1;
                return single;
            }

            keysCount = term.Length - ngramSize;
            allocator.Allocate(keysCount * (term.Length + ngramSize), out var outputBufferSlice);

            Span<byte> buffer = stackalloc byte[term.Length + ngramSize];
            term.CopyTo(buffer[ngramSize..]); // Copy the last part of the key that we will be using as potential suggestion.

            // CHECK: This may not work on compound multibyte characters. 
            var outputBuffer = outputBufferSlice.ToSpan();
            for (int i = ngramSize; i < term.Length; i++)
            {
                // Copy the ngram to the local buffer
                term[(i - ngramSize)..i].CopyTo(buffer);
                // Copy the local modified buffer
                buffer.CopyTo(outputBuffer);
                // Advance the pointer for the local buffer
                outputBuffer = outputBuffer[buffer.Length..];
            }

            return outputBufferSlice;
        }

    }
}
