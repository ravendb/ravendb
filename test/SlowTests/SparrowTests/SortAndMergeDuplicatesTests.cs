using System;
using System.Linq;
using FastTests;
using SlowTests.Utils;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SparrowTests;

public class SortAndMergeDuplicatesTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Memory)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void MinOnDuplicatesAndSortShouldWork(int seed)
    {
        var random = new Random(seed);
        var totalElementsNumber = random.Next(1, 1024);
        var uniqueElementsNumber = (int)Math.Ceiling(totalElementsNumber * 0.9);
        
        // Generate elements (indexes)
        var uniqueElementsArray = Enumerable.Range(0, uniqueElementsNumber).Select(x => (long)x).ToArray();
        var repeatedElementsArray = random.GetItems(uniqueElementsArray, totalElementsNumber - uniqueElementsNumber);

        var arrayWithRepetitions = uniqueElementsArray.Concat(repeatedElementsArray).ToArray();
        
        // Generate values (similarities)
        var values = Enumerable.Range(0, totalElementsNumber).Select(_ => (float)random.NextDouble()).ToArray();
        
        // Make copies to perform equivalent calculations using LINQ
        var arrayWithRepetitionsCopy = (long[])arrayWithRepetitions.Clone();
        var valuesCopy = (float[])values.Clone();
        
        // This works in place
        var uniqueCount = Sorting.SortAndMinOnDuplicates(arrayWithRepetitions.AsSpan(), values.AsSpan());
        
        // Do the same work using LINQ
        var result = arrayWithRepetitionsCopy.Zip(valuesCopy)
            .GroupBy(x => x.First)
            .Select(group => (
                    Id: group.Key,
                    Value: group.Min(x => x.Second)
                )
            )
            .ToArray();

        var resultIds = result.Select(x => x.Id).ToArray();
        var resultValues = result.Select(x => x.Value).ToArray();

        Assert.Equal(resultIds, arrayWithRepetitions[..uniqueCount]);
        
        for (var i = 0; i < uniqueCount; i++)
        {
            Assert.Equal(resultValues[i], values[i], 4);
        }
    }
}
