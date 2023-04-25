using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using Corax.Pipeline.Parsing;
using FastTests.Client;
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
            Assert.Equal(isAsciiRef, ScalarParsing.ValidateAscii(bytes));
            if (Sse41.IsSupported)
                Assert.Equal(isAsciiRef, SseParsing.ValidateSse41Ascii(bytes));
            if (Sse2.IsSupported)
                Assert.Equal(isAsciiRef, SseParsing.ValidateSse2Ascii(bytes));

            Assert.Equal(isAsciiRef, Parsing.ValidateAscii(bytes));
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

            Assert.Equal(expectedLength, ScalarParsing.CountCodePointsFromUtf8(bytes));
            if (Sse2.IsSupported)
                Assert.Equal(expectedLength, SseParsing.CountCodePointsFromUtf8(bytes));

            Assert.Equal(expectedLength, Parsing.CountCodePointsFromUtf8(bytes));
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

            Assert.Equal(input.Length, ScalarParsing.Utf16LengthFromUtf8(bytes));
            if (Sse2.IsSupported)
                Assert.Equal(input.Length, SseParsing.Utf16LengthFromUtf8(bytes));

            Assert.Equal(input.Length, Parsing.Utf16LengthFromUtf8(bytes));
        }
    }
}
