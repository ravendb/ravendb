using System;
using Corax.Analyzers;
using Corax.Pipeline;

namespace Corax.Indexing;

public partial class IndexWriter
{
    public class AnalyzersContext : IDisposable
    {
        public Token[] TokensBufferHandler;
        public byte[] EncodingBufferHandler;
        public byte[] Utf8ConverterBufferHandler;
      
        public AnalyzersContext(int requiredBufferSize)
        {
            EncodingBufferHandler = Analyzer.BufferPool.Rent(requiredBufferSize);
            TokensBufferHandler = Analyzer.TokensPool.Rent(requiredBufferSize);
            Utf8ConverterBufferHandler = Analyzer.BufferPool.Rent(requiredBufferSize * 10);
        }
        
        public void UnlikelyGrowAnalyzerBuffer(int newBufferSize, int newTokenSize)
        {
            if (newBufferSize > EncodingBufferHandler.Length)
            {
                Analyzer.BufferPool.Return(EncodingBufferHandler);
                EncodingBufferHandler = null;
                EncodingBufferHandler = Analyzer.BufferPool.Rent(newBufferSize);
            }

            if (newTokenSize > TokensBufferHandler.Length)
            {
                Analyzer.TokensPool.Return(TokensBufferHandler);
                TokensBufferHandler = null;
                TokensBufferHandler = Analyzer.TokensPool.Rent(newTokenSize);
            }
        }
        
        public void Dispose()
        {
            if (EncodingBufferHandler != null)
            {
                Analyzer.BufferPool.Return(EncodingBufferHandler);
                EncodingBufferHandler = null;
            }

            if (TokensBufferHandler != null)
            {
                Analyzer.TokensPool.Return(TokensBufferHandler);
                TokensBufferHandler = null;
            }

            if (Utf8ConverterBufferHandler != null)
            {
                Analyzer.BufferPool.Return(Utf8ConverterBufferHandler);
                Utf8ConverterBufferHandler = null;
            }
        }
    }
}
