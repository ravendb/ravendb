using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
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

        private LuceneAnalyzerAdapter(LuceneAnalyzer analyzer, delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, void> function) : 
            base(function, default(NullTokenizer), NoTransformers)
        {
            _analyzer = analyzer;
        }

        internal static void Run(Analyzer adapter, ReadOnlySpan<byte> source, ref Span<byte> output, ref Span<Token> tokens)
        {
            var @this = (LuceneAnalyzerAdapter)adapter;
            var analyzer = @this._analyzer;

            var data = Encoding.UTF8.GetString(source);
            using (var reader = new StringReader(data))                
            {
                var sourceSpan = data.AsSpan();

                int currentOutputIdx = 0;
                var currentTokenIdx = 0;

                var stream = analyzer.TokenStream(null, reader);
                do
                {
                    var offset = stream.GetAttribute<IOffsetAttribute>();

                    int start = offset.StartOffset;
                    int length = offset.EndOffset - start;
                    if (length == 0)
                        continue; // We skip any empty token. 

                    //var type = stream.GetAttribute<ITypeAttribute>();
                    //var position = stream.GetAttribute<IPositionIncrementAttribute>();
                    var term = stream.GetAttribute<ITermAttribute>();
                    int outputLength = Encoding.UTF8.GetBytes(term.Term, output.Slice(currentOutputIdx));

                    ref var token = ref tokens[currentTokenIdx];
                    token.Offset = currentOutputIdx;
                    token.Length = (uint)outputLength;
                    token.Type = TokenType.Word;

                    // Advance the current token output.
                    currentOutputIdx += length;
                    currentTokenIdx++;
                }
                while (stream.IncrementToken());

                output = output.Slice(0, currentOutputIdx);
                tokens = tokens.Slice(0, currentTokenIdx);
            }    
        }

        public static LuceneAnalyzerAdapter Create(LuceneAnalyzer analyzer)
        {
            return new LuceneAnalyzerAdapter(analyzer, &Run);
        }
    }
}
