using System;
using Corax.Pipeline;
using Analyzer = Corax.Analyzers.Analyzer;
using LuceneAnalyzer = Lucene.Net.Analysis.Analyzer;
using Token = Corax.Pipeline.Token;

namespace Raven.Server.Documents.Indexes.Persistence.Lucene
{
    public abstract unsafe class LuceneAnalyzerAdapter : Analyzer
    {
        private static readonly ITransformer[] NoTransformers = [];
        protected readonly LuceneAnalyzer Analyzer;
        
        protected LuceneAnalyzerAdapter(LuceneAnalyzer analyzer,
            delegate*<Analyzer, ReadOnlySpan<byte>, ref Span<byte>, ref Span<Token>, ref byte[], void> functionUtf8,
            delegate*<Analyzer, ReadOnlySpan<char>, ref Span<char>, ref Span<Token>, void> functionUtf16) : 
            base(functionUtf8, functionUtf16, default(NullTokenizer), NoTransformers)
        {
            Analyzer = analyzer;
        }

        internal static void Run(Analyzer adapter, ReadOnlySpan<char> source, ref Span<char> output, ref Span<Token> tokens)
        {
            // PERF: Currently we are not going to be supporting the use of UTF-16 in the Lucene Analyzer Adapter. 
            //       A proper implementation of SpanTextReader for ReadOnlySpan<char> should be built in order to
            //       avoid the GetBytes conversions required. 
            throw new NotImplementedException();
        }

        /// <summary>
        /// Creates instance of Corax analyzer with underlying lucene analyzer.
        /// </summary>
        public static LuceneAnalyzerAdapter Create(LuceneAnalyzer analyzer, bool forQuerying)
        {
            // Corax persists known field analyzers, which are shared between callers. 
            // However, considering that we have a guarantee that the IndexWriter is used only by a single thread,
            // we can apply a couple of optimizations to be faster for bulk operations. We are separating the implementation
            // for querying (REQUIREMENT: THREAD-SAFE) and writing (SINGLE THREAD GUARANTEE).
            if (forQuerying)
            {
                return LuceneAnalyzerAdapterForQuerying.Create(analyzer);
            }
            
            return LuceneAnalyzerAdapterForWriter.Create(analyzer);
        }
    }
}
