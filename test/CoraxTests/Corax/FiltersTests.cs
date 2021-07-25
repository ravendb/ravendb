using System;
using System.Collections.Generic;
using System.Text;
using Corax;
using Corax.Filters;
using Corax.Tokenizers;
using Xunit;
using Xunit.Abstractions;

namespace CoraxTests
{
    public class FiltersTests : NoDisposalNeeded
    {
        public FiltersTests(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData("This iS A leaDIng whitespaCE.", new[] { 4, 2, 1, 7, 11 })]
        [InlineData("This IS a trailing whitespace     ", new[] { 4, 2, 1, 8, 10 })]
        [InlineData("No_Whitespaces", new[] { 14 })]
        public void ExecuteLowercase(string value, int[] tokenSizes)
        {
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, value);

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);

            var filter = new LowerCaseFilter<WhitespaceTokenizer<StringTextSource>.Enumerator>(context);

            int tokenCount = 0;
            foreach (var token in filter.Filter(tokenizer.Tokenize(source)))
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                var tokenString = new string(Encoding.UTF8.GetChars(context.RequestReadAccess(token).ToArray()));
                Assert.Equal(tokenString.ToLower(), tokenString);

                tokenCount++;
            }

            Assert.Equal(tokenSizes.Length, tokenCount);
        }

        [Theory]
        [InlineData("This is a leading whitespace.")]
        [InlineData("No_Whitespaces")]
        public void ExecuteCachingFilter(string value)
        {
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, value);

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);

            var filter = new CachingTokenFilter<WhitespaceTokenizer<StringTextSource>.Enumerator>(context);

            var enumerator = filter.Filter(tokenizer.Tokenize(source));

            List<TokenSpan> results1 = new();
            foreach (var token in enumerator)
                results1.Add(token);

            enumerator.Reset();

            List<TokenSpan> results2 = new();
            foreach (var token in enumerator)
                results2.Add(token);

            Assert.Equal(results1.Count, results2.Count);
            for (int i = 0; i < results1.Count; i++)
                Assert.Equal(results1[i], results2[i]);
        }

        public sealed class MyStopTokenFilter<TSource> : FilteringTokenFilter<TSource>
            where TSource : IEnumerator<TokenSpan>
        {
            public MyStopTokenFilter(TokenSpanStorageContext storage) : base(storage)
            {}

            protected override bool AcceptToken(in TokenSpan token)
            {
                if (token.Length <= 2)
                    return false;

                return true;
            }
        }


        [Theory]
        [InlineData("This iS A leaDIng whitespaCE.", new[] { 4, 7, 11 })]
        [InlineData("This IS a trailing whitespace     ", new[] { 4, 8, 10 })]
        [InlineData("No_Whitespaces", new[] { 14 })]
        public void ExecuteMyStopFilter(string value, int[] tokenSizes)
        {
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, value);

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);

            var filter = new MyStopTokenFilter<WhitespaceTokenizer<StringTextSource>.Enumerator>(context);

            int tokenCount = 0;
            foreach (var token in filter.Filter(tokenizer.Tokenize(source)))
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                tokenCount++;
            }

            Assert.Equal(tokenSizes.Length, tokenCount);
        }
    }
}
