using System.Linq;
using FastTests.Voron.FixedSize;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Impl;
using Voron;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Bugs;

public class RavenDB_21087 : NoDisposalNeeded
{
    public RavenDB_21087(ITestOutputHelper output) : base(output)
    {
    }

    [Theory]
    [InlineData(3)]
    [InlineData(16)]
    [InlineData(128)]
    [InlineData(129)]
    [InlineData(130)]
    [InlineDataWithRandomSeed]
    public void CanAddAndGetValues(int numberOfItems)
    {
        if (numberOfItems > 1025)
            numberOfItems %= 1025;

        using (var allocator = new ByteStringContext(SharedMultipleUseFlag.None))
        using (var sut = new SliceSmallSet<object>(128))
        {
            var count = 0;

            for (int i = 0; i < numberOfItems; i++)
            {
                Slice.From(allocator, "test" + i, ByteStringType.Immutable, out var key);

                sut.Add(key, new object());

                count++;
            }

            var values = sut.Values.ToArray();
            Assert.Equal(count, values.Length);

            foreach (object value in values)
            {
                Assert.NotNull(value);
            }

            for (int i = 0; i < numberOfItems; i++)
            {
                using var _ = Slice.From(allocator, "test" + i, ByteStringType.Immutable, out var key);

                var result = sut.TryGetValue(key, out var value);

                Assert.True(result);
            }

            sut.Clear();

            Slice.From(allocator, "foo", ByteStringType.Immutable, out var keyFoo);

            sut.Add(keyFoo, new object());

            values = sut.Values.ToArray();
            Assert.Equal(1, values.Length);
        }
    }
}
