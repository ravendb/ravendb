using System;
using System.Runtime.CompilerServices;
using System.Text;
using Sparrow.Server;

namespace Corax.Utils
{
    internal static class SuggestionsKeys
    {
        internal static ByteString Generate(ByteStringContext allocator, int ngramSize, ReadOnlySpan<byte> term, Span<int> termsLength, out int keysCount)
        {
            // This method will generate the ngram based suggestion keys. The idea behind this keys is that we could perform
            // a very efficient starts-with operation on the suggestions tree in order to find the proper documents.
            // Keys are been constructed in such a way that common ngrams gets pushed at the start of the key. 
            // The general format is: {ngram}:{term} 
            // Therefore now we can go insert such a key and perform multiple calls to start-with with the different ngrams
            // to find all potential terms that share that ngram.

            // Since we are going to be testing against n-grams of minimum size of 2, we should be aware
            // that the last part of the work should be indexed with 2-grams and 3-grams too, to ensure
            // proper coverage at the time of doing the preselection. 

            keysCount = term.Length - 1;
            allocator.Allocate(keysCount * (term.Length + ngramSize + 1), out var outputBufferSlice);

            // CHECK: This may not work on compound multibyte characters. 
            var outputBuffer = outputBufferSlice.ToSpan();
            for (int i = 0; i < term.Length - 1; i++)
            {
                int size = Math.Min(term.Length - i, Constants.Suggestions.DefaultNGramSize);

                term.Slice(i, size)
                    .CopyTo(outputBuffer);
                outputBuffer[size] = (byte)':';
                term.CopyTo(outputBuffer.Slice(size+1));

                int termLength = term.Length + size + 1;
                termsLength[i] = termLength;

                // Advance the pointer for the local buffer
                outputBuffer = outputBuffer.Slice(termLength);
            }

            return outputBufferSlice;
        }

    }
}
