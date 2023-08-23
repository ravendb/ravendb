using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Pipeline;
using Lucene.Net.Analysis.Tokenattributes;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public sealed unsafe class LuceneAnalyzerAdapter : Analyzer
    {
        private static readonly ITransformer[] NoTransformers = new ITransformer[0];

        private readonly LuceneAnalyzer _analyzer;

        private LuceneAnalyzerAdapter(LuceneAnalyzer analyzer,
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> functionUtf8,
            delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> functionUtf16) : 
            base(functionUtf8, functionUtf16, default(NullTokenizer), NoTransformers)
        {
            _analyzer = analyzer;
        }

        internal static void Run(Analyzer adapter, ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens, ref byte[] buffer)
        {
            var @this = (LuceneAnalyzerAdapter)adapter;
            var analyzer = @this._analyzer;

            var data = Encoding.UTF8.GetString(source);
            using (var reader = new StringReader(data))                
            {
                int currentOutputIdx = 0;
                var currentTokenIdx = 0;

                var stream = analyzer.ReusableTokenStream(null, reader);
                do
                {
                    var offset = stream.GetAttribute<IOffsetAttribute>();
                    int start = offset.StartOffset;
                    int length = offset.EndOffset - start;
                    if (length == 0)
                        continue; // We skip any empty token. 

                    var term = stream.GetAttribute<ITermAttribute>();
                    int outputLength = Encoding.UTF8.GetBytes(term.TermBuffer()[..term.TermLength()], output[currentOutputIdx..]);

                    ref var token = ref tokens[currentTokenIdx];
                    token.Offset = currentOutputIdx;
                    token.Length = (uint)outputLength;
                    token.Type = TokenType.Word;

                    // Advance the current token output.
                    currentOutputIdx += outputLength;
                    currentTokenIdx++;
                }
                while (stream.IncrementToken());

                output = output.Slice(0, currentOutputIdx);
                tokens = tokens.Slice(0, currentTokenIdx);
            }
        }

        internal static void Run(Analyzer adapter, ReadOnlySpan<char> source, ref Span<char> output, ref Span<Token> tokens)
        {
            // PERF: Currently we are not going to be supporting the use of UTF-16 in the Lucene Analyzer Adapter. 
            //       A proper implementation of SpanTextReader for ReadOnlySpan<char> should be built in order to
            //       avoid the GetBytes conversions required. 
            throw new NotImplementedException();
        }

        public static LuceneAnalyzerAdapter Create(LuceneAnalyzer analyzer)
        {
            return new LuceneAnalyzerAdapter(analyzer, &Run, &Run);
        }
    }
}
