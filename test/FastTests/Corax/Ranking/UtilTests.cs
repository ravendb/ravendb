using System;
using System.Diagnostics;
using System.Linq;
using Corax.Utils;
using FastTests.Voron;
using Sparrow.Extensions;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Threading;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Ranking;

public class UtilTests : StorageTest
{
    public UtilTests(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void CanSortAndRemoveDuplicatesAndOutputProperBoost()
    {
        var d1 = new long[] {6,    5,    2,    3,    1,    1,    10,   15,   12,   9};
        var s1 = new[]      {0.1f, 0.2f, 0.3f, 0.4f, 0.1f, 0.3f, 0.1f, 0.2f, 0.3f, 0.4f};
        Assert.Equal(d1.Length, s1.Length);

        var output = Sorting.SortAndRemoveDuplicatesWithScoreMerging(d1.AsSpan(), s1.AsSpan());

        var correct = d1[..output].Zip(s1[..output]).Distinct().OrderBy(i => i.First).ToArray();
        correct[0].Second = 0.4f;

        Assert.True(correct.Select(i => i.First).SequenceEqual(d1[..output]));
        Assert.True(correct.Select(i => i.Second).SequenceEqual(s1[..output]));
    }

    // [Fact]
    // public void FrequencyHolderGrowableBufferWorksJustFine()
    // {
    //     using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
    //     using var holder = new Bm25(bsc, 10);
    //     //Append 5
    //     var encodedListOfFive = Enumerable.Range(0, 5).Select(i => FrequencyUtils.Encode(i, i)).ToArray();
    //     holder.Process(encodedListOfFive, 5);
    //     for (int i = 0; i < 5; ++i)
    //     {
    //         Assert.Equal(i, encodedListOfFive[i]);
    //         Assert.Equal(i, holder.Matches[i]);
    //         Assert.True(holder.Scores[i].AlmostEquals(i));
    //     }
    //     
    //     
    //     var encodedListOfTen = Enumerable.Range(5, 15).Select(i => FrequencyUtils.Encode(i, i)).ToArray();
    //     holder.Process(encodedListOfTen, 10);
    //     var wholeCollection = encodedListOfFive.Concat(encodedListOfTen).ToArray();
    //     for (int i = 0; i < 15; ++i)
    //     {
    //         Assert.Equal(i, wholeCollection[i]);
    //
    //         Assert.Equal(i, holder.Matches[i]);
    //         Assert.True(holder.Scores[i].AlmostEquals(i));
    //     }
    // }
}
