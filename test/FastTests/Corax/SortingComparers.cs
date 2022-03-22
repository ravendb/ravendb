using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax.Queries;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class SortingComparersTests : RavenTestBase
    {
        public SortingComparersTests(ITestOutputHelper output) : base(output)
        {}


        [RavenTheory(RavenTestCategory.Corax)]
        [InlineData("a1a", "a1b", true)]
        [InlineData("a1b", "a1a", false)]
        [InlineData("x2-y08", "x2-y7", true)]
        [InlineData("x2-y08", "x2-g8", false)]
        [InlineData("1.001", "1.010", true)]
        [InlineData("1.001", "1.1", true)]
        [InlineData("1.3", "1.1", false)]
        [InlineData("rfc1.txt", "rfc822.txt", true)]
        [InlineData("rfc822.txt", "rfc2086.txt", true)]
        [InlineData(" rfc822.txt", "rfc2086.txt", true)]
        public void AscendingNaturalComparer(string input, string compareWith, bool isAscending)
        {
            var x = Encoding.UTF8.GetBytes(input);
            var y = Encoding.UTF8.GetBytes(compareWith);

            var result = SortingMatch.BasicComparers.CompareAlphanumericAscending(x, y);            
            Assert.Equal(isAscending, result < 0);
        }
    }
}
