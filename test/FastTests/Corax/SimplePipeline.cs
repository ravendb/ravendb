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
            public bool SupportUtf8 => true;

            private const string stopWord = "Stop";
            private static readonly byte[] stopWordBytes = Encoding.ASCII.GetBytes(stopWord);

            public bool Accept(ReadOnlySpan<byte> source, in Token token)
            {
                if (source.SequenceEqual(stopWordBytes))
                {
                    return false;
                }
                return true;
            }

            public bool Accept(ReadOnlySpan<char> source, in Token token)
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

            var analyzer = Analyzer.Create(this.Allocator, default(WhitespaceTokenizer), default(LowerCaseTransformer));
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

        [Theory]
        [InlineData("Antonio Moreno Taquería")]
        [InlineData("Galería del gastrónomo")]
        [InlineData("Suprêmes délices")]
        [InlineData("łódź żółćżeż")]
        [InlineData("zażółć gęślą jaźń")]
        public void BasicAnalyzerWithUtf8(string input)
        {
            Span<byte> source = Encoding.UTF8.GetBytes(input);
            ReadOnlySpan<byte> sourceLowerCased = Encoding.UTF8.GetBytes(input.ToLower());

            var analyzer = Analyzer.Create(this.Allocator, default(KeywordTokenizer), default(LowerCaseTransformer));
            analyzer.GetOutputBuffersSize(source.Length, out int bufferSize, out int tokenSize);

            Span<byte> buffer = new byte[bufferSize];
            Span<Token> tokens = new Token[tokenSize];

            analyzer.Execute(source, ref buffer, ref tokens);

            Assert.Equal(buffer.Length, (int)tokens[0].Length);
            Assert.Equal(source.Length, buffer.Length);

            Assert.True(buffer.SequenceEqual(sourceLowerCased));
        }

        [Theory]
        [InlineData("ஸஇႤკპ")]
        public void BasicAnalyzerWithUtf8WithoutLowerCasing(string input)
        {
            Span<byte> source = Encoding.UTF8.GetBytes(input);

            var analyzer = Analyzer.Create(this.Allocator, default(KeywordTokenizer), default(LowerCaseTransformer));
            analyzer.GetOutputBuffersSize(source.Length, out int bufferSize, out int tokenSize);

            Span<byte> buffer = new byte[bufferSize];
            Span<Token> tokens = new Token[tokenSize];

            analyzer.Execute(source, ref buffer, ref tokens);

            Assert.Equal(buffer.Length, (int)tokens[0].Length);
            Assert.Equal(source.Length, buffer.Length);

            Assert.True(buffer.SequenceEqual(source));
        }

        [Theory]
        [InlineData("Antonio Moreno Taquería")]
        [InlineData("Galería del gastrónomo")]
        [InlineData("Suprêmes délices")]
        [InlineData("łódź żółćżeż")]
        [InlineData("zażółć gęślą jaźń")]
        [InlineData("Quería test")]
        public void BasicAnalyzerWithUtf8WhitespaceTokenizer(string input)
        {
            Span<byte> source = Encoding.UTF8.GetBytes(input);


            var analyzer = Analyzer.Create(this.Allocator, default(WhitespaceTokenizer), default(LowerCaseTransformer));
            analyzer.GetOutputBuffersSize(source.Length, out int bufferSize, out int tokenSize);

            Span<byte> buffer = new byte[bufferSize];
            Span<Token> tokens = new Token[tokenSize];

            var words = input.Split(' ');

            analyzer.Execute(source, ref buffer, ref tokens);

            Assert.Equal(tokens.Length, words.Length);

            for (int i = 0; i < tokens.Length; i++)
            {
                var token = tokens[i];
                var word = buffer.Slice(token.Offset, (int)token.Length);
                ReadOnlySpan<byte> wordLowerCased = Encoding.UTF8.GetBytes(words[i].ToLower());
                Assert.True(wordLowerCased.SequenceEqual(word));
                Assert.Equal(wordLowerCased.Length, (int)token.Length);
            }
        }

        private struct BasicLowercaseFilter : ITokenFilter
        {
            public bool SupportUtf8 => true;

            private const string stopWord = "stop";
            private static readonly byte[] stopWordUtf8 = Encoding.ASCII.GetBytes(stopWord);

            public bool Accept(ReadOnlySpan<byte> source, in Token token)
            {
                if (source.SequenceEqual(stopWordUtf8))
                {
                    return false;
                }
                return true;
            }

            public bool Accept(ReadOnlySpan<char> source, in Token token)
            {
                if (source.SequenceEqual(stopWord))
                {
                    return false;
                }
                return true;
            }
        }

        [Fact]
        public void BasicInnerAnalyzerUtf8()
        {
            Span<byte> source = Encoding.UTF8.GetBytes("This is a SiMple stop stop tEsT");

            var analyzer = Analyzer.Create(this.Allocator, default(WhitespaceTokenizer), default(LowerCaseTransformer))
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

        [Fact]
        public void BasicInnerAnalyzerUtf16()
        {
            ReadOnlySpan<char> source = "This is a SiMple stop stop tEsT".AsSpan();

            var analyzer = Analyzer.Create(this.Allocator, default(WhitespaceTokenizer), default(LowerCaseTransformer))
                                   .With(default(FilterTransformer<BasicLowercaseFilter>));

            analyzer.GetOutputBuffersSize(source.Length, out int bufferSize, out int tokenSize);

            Span<char> buffer = new char[bufferSize];
            Span<Token> tokens = new Token[tokenSize];

            analyzer.Execute(source, ref buffer, ref tokens);

            Assert.Equal(source.Length, buffer.Length);
            Assert.Equal(5, tokens.Length);
            Assert.Equal('t', buffer[0]);
            Assert.NotEqual(source[0], buffer[0]);
            Assert.Equal('h', buffer[1]);
            Assert.Equal(source[1], buffer[1]);

            Assert.Equal('s', buffer[10]);
        }
    }
}
