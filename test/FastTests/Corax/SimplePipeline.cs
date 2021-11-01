using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Pipeline;
using FastTests.Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{

    public class SimplePipelineTest : StorageTest
    {
        public SimplePipelineTest(ITestOutputHelper output) : base(output) { }

        [Fact]
        public void WhitespaceTokenizer()
        {
            Span<byte> source = Encoding.UTF8.GetBytes("Hello Kitty");

            Span<byte> b1 = stackalloc byte[16];
            Span<Token> tokenSlice = stackalloc Token[16];
            Span<byte> b2 = stackalloc byte[16];

            var tokenizer = new WhitespaceTokenizer();
            var t1 = tokenSlice;
            Assert.Equal(source.Length, tokenizer.Tokenize(source, ref t1));
            Assert.Equal(2, t1.Length);
            Assert.Equal(0, t1[0].Offset);
            Assert.Equal(5u, t1[0].Length);
            Assert.Equal(TokenType.Word, t1[0].Type);
            Assert.Equal(6, t1[1].Offset);
            Assert.Equal(5u, t1[1].Length);
            Assert.Equal(TokenType.Word, t1[1].Type);

            t1 = tokenSlice;
            Assert.Equal(6, tokenizer.Tokenize(source.Slice(0, 6), ref t1));
            Assert.Equal(1, t1.Length);
            Assert.Equal(0, t1[0].Offset);
            Assert.Equal(5u, t1[0].Length);
        }

        [Fact]
        public void KeywordTokenizer()
        {
            Span<byte> source = Encoding.UTF8.GetBytes("Hello Kitty");

            Span<byte> b1 = stackalloc byte[16];
            Span<Token> tokenSlice = stackalloc Token[16];
            Span<byte> b2 = stackalloc byte[16];

            var tokenizer = new KeywordTokenizer();
            var t1 = tokenSlice;
            Assert.Equal(source.Length, tokenizer.Tokenize(source, ref t1));
            Assert.Equal(1, t1.Length);
            Assert.Equal(0, t1[0].Offset);
            Assert.Equal(source.Length, (int)t1[0].Length);
        }

        [Fact]
        public void LowerCaseTrasformer()
        {
            Span<byte> source = Encoding.UTF8.GetBytes("Hello Kitty");

            Span<byte> b1 = new byte[16];
            Span<Token> tokenSlice1 = new Token[16];
            Span<byte> b2 = new byte[16];
            Span<Token> tokenSlice2 = new Token[16];

            var tokenizer = new WhitespaceTokenizer();
            tokenizer.Tokenize(source, ref tokenSlice1);

            var transformer = new LowerCaseTransformer();
            transformer.Transform(source, tokenSlice1, ref source, ref tokenSlice2);

            Assert.Equal((byte)'h', source[0]);
            Assert.Equal((byte)'e', source[1]);
            Assert.Equal((byte)' ', source[5]);
            Assert.Equal((byte)'k', source[6]);
            Assert.Equal((byte)'i', source[7]);
        }

        private struct BasicFilter : ITokenFilter
        {
            private static readonly byte[] stopWord = Encoding.ASCII.GetBytes("Stop");

            public bool Accept(ReadOnlySpan<byte> source, in Token token)
            {
                if (source.SequenceEqual(stopWord))
                {
                    return false;
                }
                return true;
            }
        }

        [Fact]
        public void BasicFiltering()
        {
            Span<byte> source = Encoding.UTF8.GetBytes("This is a simple Stop Stop test");

            Span<byte> b1 = new byte[16];
            Span<Token> tokenSlice1 = new Token[16];

            var tokenizer = new WhitespaceTokenizer();
            tokenizer.Tokenize(source, ref tokenSlice1);

            var filter = new FilteringTokenFilter<BasicFilter>();
            filter.Filter(source, ref tokenSlice1);
            Assert.Equal(5, tokenSlice1.Length);
        }

        [Fact]
        public void BasicAnalyzer()
        {
            Span<byte> source = Encoding.UTF8.GetBytes("This is a SiMple tEsT");

            var analyzer = Analyzer.Create(default(WhitespaceTokenizer), default(LowerCaseTransformer));
            analyzer.GetOutputBuffersSize(source.Length, out int bufferSize, out int tokenSize);

            Span<byte> buffer = new byte[bufferSize];
            Span<Token> tokens = new Token[tokenSize];

            analyzer.Execute(source, ref buffer, ref tokens);

            Assert.Equal(source.Length, buffer.Length);
            Assert.Equal(5, tokens.Length);
            Assert.Equal((byte)'t', buffer[0]);
            Assert.NotEqual(source[0], buffer[0]);
            Assert.Equal((byte)'h', buffer[1]);
            Assert.Equal(source[1], buffer[1]);

            Assert.Equal((byte)'s', buffer[10]);
        }

        private struct BasicLowercaseFilter : ITokenFilter
        {
            private static readonly byte[] stopWord = Encoding.ASCII.GetBytes("stop");

            public bool Accept(ReadOnlySpan<byte> source, in Token token)
            {
                if (source.SequenceEqual(stopWord))
                {
                    return false;
                }
                return true;
            }
        }

        [Fact]
        public void BasicInnerAnalyzer()
        {
            Span<byte> source = Encoding.UTF8.GetBytes("This is a SiMple stop stop tEsT");

            var analyzer = Analyzer.Create(default(WhitespaceTokenizer), default(LowerCaseTransformer))
                                   .With(default(FilterTransformer<BasicLowercaseFilter>));     
            
            analyzer.GetOutputBuffersSize(source.Length, out int bufferSize, out int tokenSize);

            Span<byte> buffer = new byte[bufferSize];
            Span<Token> tokens = new Token[tokenSize];

            analyzer.Execute(source, ref buffer, ref tokens);

            Assert.Equal(source.Length, buffer.Length);
            Assert.Equal(5, tokens.Length);
            Assert.Equal((byte)'t', buffer[0]);
            Assert.NotEqual(source[0], buffer[0]);
            Assert.Equal((byte)'h', buffer[1]);
            Assert.Equal(source[1], buffer[1]);

            Assert.Equal((byte)'s', buffer[10]);
        }
    }
}
