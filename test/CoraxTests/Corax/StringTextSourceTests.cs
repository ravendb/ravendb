using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using Corax.Tokenizers;
using Xunit;
using Xunit.Abstractions;

namespace CoraxTests
{

    public class StringTextSourceTests : NoDisposalNeeded
    {
        public StringTextSourceTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void SimpleTokenization()
        {
            var context = new TokenSpanStorageContext();
            var source = new StringTextSource(context, "This is a good string.");

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);

            int[] tokenSizes = { 4, 2, 1, 4, 7 };

            int tokenCount = 0;
            foreach (var token in tokenizer.Tokenize(source))
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                tokenCount++;
            }

            Assert.Equal(5, tokenCount);
        }

        [Fact]
        public void ResetSource()
        {
            var context = new TokenSpanStorageContext();
            var source1 = new StringTextSource(context, "This is a good string.");
            var source2 = new StringTextSource(context, "This is a another string.");

            var tokenizer = new WhitespaceTokenizer<StringTextSource>(context);

            // Iterate the first source.
            foreach (var token in tokenizer.Tokenize(source1)) { }

            int[] tokenSizes = { 4, 2, 1, 7, 7 };

            int tokenCount = 0;
            foreach (var token in tokenizer.Tokenize(source2))
            {
                Assert.Equal(tokenSizes[tokenCount], token.Length);
                tokenCount++;
            }

            Assert.Equal(5, tokenCount);
        }
    }
}
