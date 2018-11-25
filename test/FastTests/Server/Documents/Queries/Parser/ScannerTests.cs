using System.Globalization;
using Raven.Server.Documents.Queries.AST;
using Xunit;
using Raven.Server.Documents.Queries.Parser;

namespace FastTests.Server.Documents.Queries.Parser
{
    public class ScannerTests : NoDisposalNeeded
    {
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        [InlineData("  \t ")]
        public void EmptyQueriesShouldJustReturnEndOfInput(string q)
        {
            var qs = new QueryScanner();
            qs.Init(q);

            Assert.False(qs.Identifier());
        }

        [Theory]
        [InlineData("hello", 0, 5)]
        [InlineData(" name = ", 1, 4)]
        [InlineData("some_thing ", 0, 10)]
        public void IdentifierShouldBeFound(string q, int start, int len)
        {
            var qs = new QueryScanner();
            qs.Init(q);

            Assert.True(qs.Identifier());
            Assert.Equal(start, qs.Token.Offset);
            Assert.Equal(len, qs.Token.Length);
        }


        [Theory]
        [InlineData(" 'hel lo' ", "hel lo")]
        [InlineData(" \"he \"", "he ")]
        [InlineData(" \"he\"\" \" ", "he\" ")]
        [InlineData(" 'we''ll' ", "we'll")]
        [InlineData(" 'we\r\nll' ", "we\r\nll")]
        public void ParseStringLiterals(string q, string escape)
        {
            var qs = new QueryScanner();
            qs.Init(q);

            Assert.True(qs.String(out var a));
            Assert.Equal(escape, a);
        }

        [Theory]
        [InlineData("1", 1L)]
        [InlineData("1.0", 1.0D)]
        [InlineData("1234 ", 1234L)]
        [InlineData(" -1234 ", -1234L)]
        [InlineData(" 1.3", 1.3D)]
        [InlineData(" -1.32", -1.32D)]
        public void ParseNumbers(string q, object expected)
        {
            var qs = new QueryScanner();
            qs.Init(q);

            var result = qs.TryNumber();
            Assert.NotNull(result);
            if (result == NumberToken.Double)
                Assert.Equal((double)expected, double.Parse(qs.Token.Value, CultureInfo.InvariantCulture));
            else
                Assert.Equal((long)expected, long.Parse(qs.Token.Value, CultureInfo.InvariantCulture));
        }

        [Theory]
        [InlineData("hello there", 6, 5)]
        public void CanScanConsecutiveIdentifiers(string q, int start, int len)
        {
            var qs = new QueryScanner();
            qs.Init(q);

            Assert.True(qs.Identifier());
            Assert.True(qs.Identifier());
            Assert.Equal(start, qs.Token.Offset);
            Assert.Equal(len, qs.Token.Length);
        }
    }
}
