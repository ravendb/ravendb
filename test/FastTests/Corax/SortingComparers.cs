using System.Text;
using Corax.Querying.Matches.SortingMatches;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class SortingComparersTests : NoDisposalNeeded
    {
        public SortingComparersTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("a1a", "a1b", true)]
        [InlineData("fields-51-A", "fields-507-A", true)]
        [InlineData("a1b", "a1a", false)]
        [InlineData("x2-y08", "x2-y7", true)]
        [InlineData("x2-y08", "x2-g8", false)]
        [InlineData("1.001", "1.010", true)]
        [InlineData("1.001", "1.1", true)]
        [InlineData("1.3", "1.1", false)]
        [InlineData("rfc1.txt", "rfc822.txt", true)]
        [InlineData("rfc822.txt", "rfc2086.txt", true)]
        [InlineData(" rfc822.txt", "rfc2086.txt", true)]
        [InlineData("\u0245rfc822.txt", "\u0245rfc2086.txt", true)]
        [InlineData("\u0245x2-y08", "\u0245x2-y7", true)]
        [InlineData("x2-y\u0245\u0244", "x2-y\u0245\u0245", true)]
        [InlineData("A1a", "a1a", false)]
        public void AscendingNaturalComparer(string input, string compareWith, bool isAscending)
        {
            var x = Encoding.UTF8.GetBytes(input);
            var y = Encoding.UTF8.GetBytes(compareWith);
            var result = AlphanumericalComparer.Instance.Compare(x, y, false);
            Assert.Equal(isAscending, result < 0);
        }
    }
}
