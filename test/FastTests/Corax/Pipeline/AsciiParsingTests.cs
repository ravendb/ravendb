using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using Corax.Pipeline.Parsing;
using FastTests.Voron;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using Assert = NetTopologySuite.Utilities.Assert;

namespace FastTests.Corax.Pipeline
{
    public class AsciiParsingTests : StorageTest
    {
        public AsciiParsingTests(ITestOutputHelper output) : base(output) { }


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
            Assert.IsEquals(isAsciiRef, ScalarParsing.ValidateAscii(bytes));
            if (Sse41.IsSupported)
                Assert.IsEquals(isAsciiRef, SseParsing.ValidateSse41Ascii(bytes));
            if (Sse2.IsSupported)
                Assert.IsEquals(isAsciiRef, SseParsing.ValidateSse2Ascii(bytes));

            Assert.IsEquals(isAsciiRef, Ascii.ValidateAscii(bytes));
        }
    }
}
