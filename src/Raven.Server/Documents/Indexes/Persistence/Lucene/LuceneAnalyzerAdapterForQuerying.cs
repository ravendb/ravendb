using System;
using System.IO;
using System.Text;
using Corax.Pipeline;
using Lucene.Net.Analysis.Tokenattributes;
using Analyzer = Corax.Analyzers.Analyzer;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

internal sealed unsafe class LuceneAnalyzerAdapterForQuerying : LuceneAnalyzerAdapter
{
    private LuceneAnalyzerAdapterForQuerying(LuceneAnalyzer analyzer,
        delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> functionUtf8,
        delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> functionUtf16) : base(analyzer, functionUtf8, functionUtf16)
    {
    }

    private static void Run(Analyzer adapter, ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens, ref byte[] buffer)
    {
        var @this = (LuceneAnalyzerAdapterForQuerying)adapter;
        var analyzer = @this.Analyzer;

        var data = Encoding.UTF8.GetString(source);
        using (var reader = new StringReader(data))
        {
            int currentOutputIdx = 0;
            var currentTokenIdx = 0;

            using var stream = analyzer.TokenStream(null, reader);
            do
            {
                var offset = stream.GetAttribute<IOffsetAttribute>();
                int start = offset.StartOffset;
                int length = offset.EndOffset - start;
                if (length == 0)
                    continue; // We skip any empty token. 

                var term = stream.GetAttribute<ITermAttribute>();
                int outputLength = Encoding.UTF8.GetBytes(term.Term, output.Slice(currentOutputIdx));

                ref var token = ref tokens[currentTokenIdx];
                token.Offset = currentOutputIdx;
                token.Length = (uint)outputLength;
                token.Type = TokenType.Word;

                // Advance the current token output.
                currentOutputIdx += outputLength;
                currentTokenIdx++;
            } while (stream.IncrementToken());

            output = output.Slice(0, currentOutputIdx);
            tokens = tokens.Slice(0, currentTokenIdx);
        }
    }

    internal static LuceneAnalyzerAdapterForQuerying Create(LuceneAnalyzer analyzer)
    {
        return new LuceneAnalyzerAdapterForQuerying(analyzer, &Run, &Run);
    }
}
