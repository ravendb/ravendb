using System;
using System.Collections.Generic;
using Sparrow.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class ConcurrentSetTests : NoDisposalNeeded
    {
        public ConcurrentSetTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void UnionWith_AddsAllElementsFromOtherCollection()
        {
            // Arrange
            var set = new ConcurrentSet<int> { 1, 2, 3 };
            var other = new List<int> { 4, 5, 6 };

            // Act
            set.UnionWith(other);

            // Assert
            Assert.Equal(6, set.Count);
            Assert.Contains(1, set);
            Assert.Contains(2, set);
            Assert.Contains(3, set);
            Assert.Contains(4, set);
            Assert.Contains(5, set);
            Assert.Contains(6, set);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void UnionWith_DoesNotAddDuplicates()
        {
            // Arrange
            var set = new ConcurrentSet<int> { 1, 2, 3 };
            var other = new List<int> { 3, 4, 5 };

            // Act
            set.UnionWith(other);

            // Assert
            Assert.Equal(5, set.Count);
            Assert.Contains(1, set);
            Assert.Contains(2, set);
            Assert.Contains(3, set);
            Assert.Contains(4, set);
            Assert.Contains(5, set);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void UnionWith_ThrowsArgumentNullException_WhenOtherIsNull()
        {
            // Arrange
            var set = new ConcurrentSet<int>();

            // Act & Assert
            Assert.Throws<ArgumentNullException>(() => set.UnionWith(null));
        }
    }
}
