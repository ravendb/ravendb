using System;
using System.Text;
using Corax.Pipeline;
using FastTests.Voron;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Version = Lucene.Net.Util.Version;

namespace FastTests.Server.Documents.Indexing.Corax
{
    public class LuceneAnalyzerAdapterTest : StorageTest
    {
        public LuceneAnalyzerAdapterTest(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(false)]
        [InlineData(true)]
        public void BasicStandardAnalyzer(bool forQuerying)
        {
            Span<byte> source = Encoding.UTF8.GetBytes("This is a SiMple stop stop tEsT");

            var analyzer = LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Version.LUCENE_30), forQuerying);

            Span<byte> outputBuffer = new byte[512];
            Span<Token> outputTokens = new Token[512];

            analyzer.Execute(source, ref outputBuffer, ref outputTokens);

            Assert.Equal(18, outputBuffer.Length);
            Assert.Equal(4, outputTokens.Length);
            Assert.Equal((byte)'s', outputBuffer[0]);
            Assert.Equal((byte)'s', outputBuffer[6]);
            Assert.Equal((byte)'s', outputBuffer[10]);
            Assert.Equal((byte)'t', outputBuffer[14]);

            Assert.Equal(0, outputTokens[0].Offset);
            Assert.Equal(6u, outputTokens[0].Length);
            Assert.Equal(TokenType.Word, outputTokens[0].Type);

            Assert.Equal(14, outputTokens[3].Offset);
            Assert.Equal(4u, outputTokens[3].Length);
            Assert.Equal(TokenType.Word, outputTokens[3].Type);
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData(false)]
        [InlineData(true)]
        public void StandardAnalyzerWithSampleDataUtf8(bool forQuerying)
        {
            Span<byte> source = Encoding.UTF8.GetBytes("Toms Spezialitäten");
            //Notice: Raven uses 29 version, not 30.
            var analyzer = LuceneAnalyzerAdapter.Create(new RavenStandardAnalyzer(Version.LUCENE_29), forQuerying);

            Span<byte> outputBuffer = new byte[512];
            Span<Token> outputTokens = new Token[512];

            analyzer.Execute(source, ref outputBuffer, ref outputTokens);

            Assert.Equal(2, outputTokens.Length);
            var firstWord = outputBuffer.Slice(outputTokens[0].Offset, (int)outputTokens[0].Length);
            Assert.Equal("toms", System.Text.Encoding.Default.GetString(firstWord));
            Assert.True(outputTokens[1].Offset + (int)outputTokens[1].Length <= outputBuffer.Length);
            var secondWord = outputBuffer.Slice(outputTokens[1].Offset, (int)outputTokens[1].Length);
            Assert.Equal("spezialitäten", System.Text.Encoding.Default.GetString(secondWord));
        }
    }
}
