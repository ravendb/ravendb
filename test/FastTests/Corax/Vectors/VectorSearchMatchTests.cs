using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Querying.Matches;
using FastTests.Voron.FixedSize;
using Sparrow.Server.Utils.VxSort;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax.Vectors;

public class VectorSearchMatchTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Vector)]
    public void CanHandleEmpty()
    {
        var result = VectorSearchMatch.RemoveDuplicates([], []);
        Assert.Equal(0, result);
    }
    
    [RavenFact(RavenTestCategory.Vector)]
    public void CanReturnSingle()
    {
        long[] matches = [1L];
        float[] distances = [.5f];

        var result = VectorSearchMatch.RemoveDuplicates(matches, distances);
        Assert.Equal(1, result);
    }

    [RavenFact(RavenTestCategory.Vector)]
    public void AllRepetitions()
    {
        long[] matches = [1L, 1L, 1L, 1L, 1L];
        float[] distances = [0.7f, 0.1f, 0.2f, 0.3f, 0.8f];

        var result = VectorSearchMatch.RemoveDuplicates(matches, distances);
        Assert.Equal(1, result);
        Assert.Equal(1L, matches[0]);
        Assert.Equal(0.1f, distances[0], 0.001);
    }
    
    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineData(1337)]
    public void UniqueArrayWillNotLooseAnyElements(int seed)
    {
        //no duplicate test
        var random = new Random(seed);
        var amount = random.Next(2, 1025);
        HashSet<long> matchesHashSet = new();
        while (matchesHashSet.Count != amount)
            matchesHashSet.Add(random.NextInt64(1, long.MaxValue));
        
        var distances = Enumerable.Range(0, amount).Select(_ => random.NextSingle()).ToArray();
        var matches = matchesHashSet.ToArray();

        var originalDistances = distances.ToArray();
        var originalMatches = matches.ToArray();
        
        matches.AsSpan().Sort(distances.AsSpan());
        var result = VectorSearchMatch.RemoveDuplicates(matches, distances);
        Assert.Equal(amount, result);

        originalMatches.AsSpan().Sort(originalDistances.AsSpan());
        
        Assert.Equal(originalMatches, matches);
        Assert.Equal(originalDistances, distances);
    }

    [RavenTheory(RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineData(1337)]
    public void NonUniqueArrayWillOutputWithRepetitionWithLowestDistance(int seed)
    {
        var random = new Random(seed);
        var amount = random.Next(2, 1025);
        //0.25
        var matches = new long[amount];
        var distances = new float[amount];
        var fromRandomAmount = (int)(amount * .75f);
        for (int i = 0; i < amount * 0.75; ++i)
        {
            matches[i] = random.NextInt64(1, long.MaxValue);
            distances[i] = random.NextSingle();
        }
        
        random.GetItems(matches[..fromRandomAmount], matches.AsSpan()[fromRandomAmount..]);
        for (int i = fromRandomAmount; i < amount; ++i)
            distances[i] = random.NextSingle();

        matches.AsSpan().Sort(distances.AsSpan());

        var originalMatches = matches.ToArray();
        var originalDistances = distances.ToArray();
        var result = VectorSearchMatch.RemoveDuplicates(matches, distances);

        Dictionary<long, float > idToSmallestDistance = new();
        for (int i = 0; i < amount; ++i)
        {
            ref var value = ref CollectionsMarshal.GetValueRefOrAddDefault(idToSmallestDistance, originalMatches[i], out bool exists);
            
                value = exists
                ? Math.Min(value, originalDistances[i]) 
                : originalDistances[i];
        }

        var resultTransformation = idToSmallestDistance
            .Select(t => (t.Key, t.Value))
            .OrderBy(t => t.Key).ThenBy(t => t.Value)
            .ToArray();
        
        Assert.Equal(resultTransformation.Length, result);
        Assert.Equal(resultTransformation.Select(t => t.Key), matches[..result]);
        Assert.Equal(resultTransformation.Select(t => t.Value), distances[..result]);
    }
}
