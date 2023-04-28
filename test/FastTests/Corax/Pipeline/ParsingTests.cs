using System;
using System.Runtime.Intrinsics.X86;
using System.Text;
using Corax.Pipeline;
using Corax.Pipeline.Parsing;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Pipeline
{
    public class ParsingTests : StorageTest
    {
        public ParsingTests(ITestOutputHelper output) : base(output) { }


        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, this is an ASCII - only test string!")]
        [InlineData("こんにちは, this string contains non-ASCII characters!")]
        [InlineData("The quick brown fox jumps over the lazy dog. Zażółć gęślą jaźń, 你好, Привет!")]
        public void AsciiDetect(string stringToCheck)
        {
            bool IsAsciiRef(byte[] input)
            {
                foreach (byte b in input)
                {
                    if (b >= 0b10000000)
                        return false;
                }
                return true;
            }

            var bytes = Encoding.UTF8.GetBytes(stringToCheck);

            var isAsciiRef = IsAsciiRef(bytes);
            Assert.Equal(isAsciiRef, ScalarParsers.ValidateAscii(bytes));
            if (Sse41.IsSupported)
                Assert.Equal(isAsciiRef, VectorParsers.ValidateSse41Ascii(bytes));
            if (Sse2.IsSupported)
                Assert.Equal(isAsciiRef, VectorParsers.ValidateSse2Ascii(bytes));

            Assert.Equal(isAsciiRef, StandardParsers.ValidateAscii(bytes));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!", 13)]
        [InlineData("Привет, мир!", 12)]
        [InlineData("こんにちは、世界！", 9)]
        [InlineData("🙂🙃😀😃", 4)]
        [InlineData("안녕하세요, 세계!", 10)]
        [InlineData("The quick brown 🦊 jumps over the lazy 🐶. What a wonderful day! ", 63)]
        [InlineData("One day, a terrible dragon 🐉 attacked the kingdom, and the queen had to use her magical powers to save her people. ", 115)]
        public void Utf8Length(string input, int expectedLength)
        {
            var bytes = Encoding.UTF8.GetBytes(input);

            Assert.Equal(expectedLength, ScalarParsers.CountCodePointsFromUtf8(bytes));
            if (Sse2.IsSupported)
                Assert.Equal(expectedLength, VectorParsers.CountCodePointsFromUtf8(bytes));

            Assert.Equal(expectedLength, StandardParsers.CountCodePointsFromUtf8(bytes));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!")]
        [InlineData("Привет, мир!")]
        [InlineData("こんにちは、世界！")]
        [InlineData("🙂🙃😀😃")]
        [InlineData("안녕하세요, 세계!")]
        [InlineData("The quick brown \U0001f98a jumps over the lazy 🐶. What a wonderful day! ")]
        [InlineData("One day, a terrible dragon 🐉 attacked the kingdom, and the queen had to use her magical powers to save her people. ")]
        public void Utf16LengthFromUtf8(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);

            Assert.Equal(input.Length, ScalarParsers.Utf16LengthFromUtf8(bytes));
            if (Sse2.IsSupported)
                Assert.Equal(input.Length, VectorParsers.Utf16LengthFromUtf8(bytes));

            Assert.Equal(input.Length, StandardParsers.Utf16LengthFromUtf8(bytes));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!")]
        [InlineData("Hello\tWorld\n")]
        [InlineData("\u000B\f\r\u001C\u001D\u001E\u001F")]
        [InlineData("\t\n\u000B\f\r\u001C\u001D\u001E\u001F\t")]
        [InlineData("Whitespace\tat\nthe\u000Bend\f")]
        public void CountWhitespaces(string input)
        {
            static int CountWhitespacesRef(ReadOnlySpan<byte> buffer)
            {
                int whitespaceCount = 0;

                foreach (byte b in buffer)
                {
                    switch (b)
                    {
                        case (byte)'\t':
                        case (byte)'\n':
                        case (byte)'\f':
                        case (byte)'\r':
                        case (byte)'\u000B':
                        case (byte)'\u001C':
                        case (byte)'\u001D':
                        case (byte)'\u001E':
                        case (byte)'\u001F':
                            whitespaceCount++;
                            break;
                    }
                }

                return whitespaceCount;
            }

            var bytes = Encoding.UTF8.GetBytes(input);
            var referenceLength = CountWhitespacesRef(bytes);

            Assert.Equal(referenceLength, ScalarParsers.CountWhitespacesAscii(bytes));
            if (Sse2.IsSupported)
                Assert.Equal(referenceLength, VectorParsers.CountWhitespacesAscii(bytes));

            Assert.Equal(referenceLength, StandardParsers.CountWhitespacesAscii(bytes));
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("Hello, World!", 2)]
        [InlineData("Hello,  World!", 2)]
        [InlineData("Привет, мир!", 2)]
        [InlineData("こんにちは、世界！", 1)]
        [InlineData("🙂🙃😀😃", 1)]
        [InlineData("안녕하세요, 세계!", 2)]
        [InlineData("The quick brown \U0001f98a jumps over the lazy 🐶. What a wonderful day! ", 13)]
        [InlineData("One day, a terrible dragon 🐉 attacked the kingdom, and the queen had to use her magical powers to save her people. ", 22)]
        [InlineData("One day, a terrible dragon  attacked the kingdom,  and the queen had to use her magical powers to save her people. ", 21)]
        [InlineData("\u000B\f\r\u001C\u001D\u001E\u001F", 0)]
        [InlineData("\u0009\u000A\u000B\u000C\u000D\u001C\u001D\u001E\u001F ", 0)]
        [InlineData("Whitespace\tat\nthe\u000Bend\f", 4)]
        [InlineData("Hello,                                                          World!", 2)]
        [InlineData("    Hello,             World!", 2)]
        [InlineData("", 0)]
        [InlineData("                        ", 0)]
        public void WhitespaceTokenizer(string input, int expectedTokens)
        {
            var bytes = Encoding.UTF8.GetBytes(input);

            var tokenArray = new Token[128];

            var tokens = tokenArray.AsSpan();
            ScalarTokenizers.TokenizeWhitespaceAsciiScalar(bytes, ref tokens);
            Assert.Equal(expectedTokens, tokens.Length);
            if (Sse2.IsSupported)
            {
                tokens = tokenArray.AsSpan();
                VectorTokenizers.TokenizeWhitespaceAsciiSse(bytes, ref tokens);
                Assert.Equal(expectedTokens, tokens.Length);
            }

            tokens = tokenArray.AsSpan();
            ScalarTokenizers.TokenizeWhitespace(input, ref tokens);
            Assert.Equal(expectedTokens, tokens.Length);
        }
    }
}
