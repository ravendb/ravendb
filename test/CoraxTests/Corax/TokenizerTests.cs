using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Tokenizers;
using Xunit;
using Xunit.Abstractions;

namespace CoraxTests
{
    public class TokenizerTests : NoDisposalNeeded
    {
        public TokenizerTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("      This is a leading whitespace", new[] { 4, 2, 1, 7, 10 })]
        [InlineData("This is a trailing whitespace     ", new[] { 4, 2, 1, 8, 10 })]
        [InlineData("No_whitespaces", new[] { 14 })]
        public void ParseWhitespaces(string value, int[] tokenSizes)
        {
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, value);

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);

            int tokenCount = 0;
            foreach (var token in tokenizer.Tokenize(source))
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                tokenCount++;
            }

            Assert.Equal(tokenSizes.Length, tokenCount);
        }

        [Theory]
        [InlineData("No_whitespaces", 1)]
        [InlineData("No_whitespaces", 10)]
        public void ParseKeywords(string value, int repetitions)
        {
            for (int i = 0; i < repetitions; i++)
                value += value;
            
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, value);

            var tokenizer = new KeywordTokenizer<StringTextSource>(context);

            int tokenCount = 0;
            foreach (var token in tokenizer.Tokenize(source))
            {
                Assert.Equal(value.Length, token.Length);
                tokenCount++;
            }
        }
    }

}
