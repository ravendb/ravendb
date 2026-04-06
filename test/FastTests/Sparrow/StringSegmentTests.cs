using System;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class StringSegmentTests(ITestOutputHelper output) : NoDisposalNeeded(output)
    {
        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("abc", "abc")]
        [InlineData("hello", "hello")]
        [InlineData("x", "x")]
        public void EndsWith_FullMatch_ReturnsTrue(string source, string suffix)
        {
            var segment = new StringSegment(source);
            Assert.True(segment.EndsWith(suffix, StringComparison.Ordinal));
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("hello world", "world")]
        [InlineData("abc", "bc")]
        [InlineData("abc", "c")]
        public void EndsWith_PartialMatch_ReturnsTrue(string source, string suffix)
        {
            var segment = new StringSegment(source);
            Assert.True(segment.EndsWith(suffix, StringComparison.Ordinal));
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("abc", "xyz")]
        [InlineData("abc", "abcd")]
        public void EndsWith_NoMatch_ReturnsFalse(string source, string suffix)
        {
            var segment = new StringSegment(source);
            Assert.False(segment.EndsWith(suffix, StringComparison.Ordinal));
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("hello world", 6, 5, "world")]
        public void EndsWith_WithOffset_FullMatch_ReturnsTrue(string buffer, int offset, int length, string suffix)
        {
            var segment = new StringSegment(buffer, offset, length);
            Assert.True(segment.EndsWith(suffix, StringComparison.Ordinal));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void EndsWith_WithOffset_DoesNotMatchOutsideSegment()
        {
            var segment = new StringSegment("hello world", 6, 5);
            Assert.False(segment.EndsWith(" world", StringComparison.Ordinal));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void LastIndexOf_EmptySegment_ReturnsNegativeOne()
        {
            var segment = new StringSegment("hello", 0, 0);
            Assert.Equal(-1, segment.LastIndexOf('h'));
        }

        [RavenTheory(RavenTestCategory.Core)]
        [InlineData("hello", 'l', 3)]
        [InlineData("hello", 'h', 0)]
        [InlineData("hello", 'o', 4)]
        [InlineData("hello", 'x', -1)]
        public void LastIndexOf_ReturnsCorrectIndex(string source, char value, int expected)
        {
            var segment = new StringSegment(source);
            Assert.Equal(expected, segment.LastIndexOf(value));
        }
    }
}
