using System;
using System.IO;
using System.Text;
using Corax.Pipeline;
using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Tokenattributes;
using Raven.Server.Json;
using Analyzer = Corax.Analyzers.Analyzer;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using Token = Corax.Pipeline.Token;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene;

internal sealed unsafe class LuceneAnalyzerAdapterForQuerying : LuceneAnalyzerAdapter
{
    [ThreadStatic]
    private static LazyStringReader LazyStringReader;

    [ThreadStatic]
    private static TokenStream Stream;

    [ThreadStatic]
    private static IOffsetAttribute Offset;

    [ThreadStatic]
    private static ITermAttribute Term;
    
    private LuceneAnalyzerAdapterForQuerying(LuceneAnalyzer analyzer,
        delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> functionUtf8,
        delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> functionUtf16) : base(analyzer, functionUtf8, functionUtf16)
    {
    }

    private static void Run(Analyzer adapter, ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens, ref byte[] buffer)
    {
        var self = (LuceneAnalyzerAdapterForQuerying)adapter;
        LazyStringReader ??= new();
        var analyzer = self.Analyzer;
        var term = Term;
        var offset = Offset;

        fixed (byte* pText = source)
        {
            TextReader reader = LazyStringReader.GetTextReader(pText, source.Length);

            int currentOutputIdx = 0;
            var currentTokenIdx = 0;

            var stream = analyzer.ReusableTokenStream(null, reader);
            if (ReferenceEquals(stream, Stream) == false)
            {
                Stream = stream;
                Offset = offset = stream.GetAttribute<IOffsetAttribute>();
                Term = term = stream.GetAttribute<ITermAttribute>();
            }
            do
            {
                int start = offset.StartOffset;
                int length = offset.EndOffset - start;
                if (length == 0)
                    continue; // We skip any empty token. 

                ReadOnlySpan<char> termChars = term.TermBuffer();
                int outputLength = Encoding.UTF8.GetBytes(termChars[..term.TermLength()], output[currentOutputIdx..]);

                ref var token = ref tokens[currentTokenIdx];
                token.Offset = currentOutputIdx;
                token.Length = (uint)outputLength;
                token.Type = TokenType.Word;

                // Advance the current token output.
                currentOutputIdx += outputLength;
                currentTokenIdx++;
            } while (stream.IncrementToken());

            output = output[..currentOutputIdx];
            tokens = tokens[..currentTokenIdx];
        }
    }

    internal static LuceneAnalyzerAdapterForQuerying Create(LuceneAnalyzer analyzer)
    {
        return new LuceneAnalyzerAdapterForQuerying(analyzer, &Run, &Run);
    }
}
