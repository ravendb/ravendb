using System;
using System.Linq;
using System.Numerics;
using BenchmarkDotNet.Attributes;
using Sparrow;

namespace Micro.Benchmark.Benchmarks;

public class SortAndRemoveDuplicates
{
    [Params(16, 64, 256, 1024, 1024 * 4)]
    public int ArraySize { get; set; }

    private long[] _sourceArray, _workingArray;
    private float[] _sourceScoreArray, _workingScoreArray;
    
    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(125123);
        var totalElementsNumber = ArraySize;
        var uniqueElementsNumber = (int)Math.Ceiling(totalElementsNumber * 0.9);
        
        _sourceArray = Enumerable.Range(0, uniqueElementsNumber).Select(x => (long)x).ToArray();
        var repeatedElementsArray = random.GetItems(_sourceArray, totalElementsNumber - uniqueElementsNumber);
        _sourceArray = _sourceArray.Concat(repeatedElementsArray).ToArray();
        
        _sourceScoreArray = Enumerable.Range(0, totalElementsNumber).Select(_ => (float)random.NextDouble()).ToArray();
        
        _workingScoreArray = new float[totalElementsNumber];
        _workingArray = new long[totalElementsNumber];
    }

    [Benchmark(Baseline = true)]
    public int SortAndMergeDuplicatesWithBranch()
    {
        _sourceArray.AsSpan().CopyTo(_workingArray);
        _sourceScoreArray.AsSpan().CopyTo(_workingScoreArray);
        return Sparrow.Server.Utils.Sorting.SortAndMergeDuplicates(_workingArray.AsSpan(), _workingScoreArray.AsSpan());
    }
    
    [Benchmark]
    public int SortAndMergeDuplicatesBranchless()
    {
        _sourceArray.AsSpan().CopyTo(_workingArray);
        _sourceScoreArray.AsSpan().CopyTo(_workingScoreArray);
        return SortAndMergeDuplicatesBranchless(_workingArray.AsSpan(), _workingScoreArray.AsSpan());
    }
    
    [Benchmark]
    public int SortAndRemoveDuplicatesWithBranch()
    {
        _sourceArray.AsSpan().CopyTo(_workingArray);
        _sourceScoreArray.AsSpan().CopyTo(_workingScoreArray);
        return SortAndRemoveDuplicatesWithBranch(_workingArray.AsSpan(), _workingScoreArray.AsSpan());
    }

    [Benchmark]
    public int SortAndRemoveDuplicatesBranchless()
    {
        _sourceArray.AsSpan().CopyTo(_workingArray);
        _sourceScoreArray.AsSpan().CopyTo(_workingScoreArray);
        return Sparrow.Server.Utils.Sorting.SortAndRemoveDuplicates(_workingArray.AsSpan(), _workingScoreArray.AsSpan());
    }
        
    private static int SortAndRemoveDuplicatesWithBranch<T, W>(Span<T> valuesToDeduplicate, Span<W> itemsAssociated)
        where T : unmanaged, IBinaryNumber<T>
    {
        if (valuesToDeduplicate.Length <= 1)
            return valuesToDeduplicate.Length;
            
        valuesToDeduplicate.Sort(itemsAssociated);

        int outputIdx = 0;
        for (int i = 1; i < valuesToDeduplicate.Length; i++)
        {
            if (valuesToDeduplicate[i] == valuesToDeduplicate[outputIdx])
            {
                itemsAssociated[outputIdx] = itemsAssociated[i];
            }
            else
            {
                outputIdx++;
                valuesToDeduplicate[outputIdx] = valuesToDeduplicate[i];
                itemsAssociated[outputIdx] = itemsAssociated[i];
            }
        }

        return outputIdx + 1;
    }
    
    private static int SortAndMergeDuplicatesBranchless(Span<long> valuesToDeduplicate, Span<float> itemsAssociated)
    {
        if (valuesToDeduplicate.Length <= 1)
            return valuesToDeduplicate.Length;
            
        valuesToDeduplicate.Sort(itemsAssociated);

        // We need to fill in the gaps left by removing deduplication process.
        // If there are no duplicated the writes at the architecture level will execute
        // way faster than if there are.

        int nextI = 0;
        int outputIdx = 0;
        while (nextI < valuesToDeduplicate.Length - 1)
        {
            int i = nextI;
            nextI++;
            var nextItemHasSameId = (valuesToDeduplicate[nextI] == valuesToDeduplicate[i]).ToInt32();
            var nextItemHasDifferentId = nextItemHasSameId ^ 1;
                
            // When elements are equal, add previous element to next
            itemsAssociated[nextI] += nextItemHasSameId * itemsAssociated[outputIdx];
            outputIdx += nextItemHasDifferentId;
                
            valuesToDeduplicate[outputIdx] = valuesToDeduplicate[nextI];
            itemsAssociated[outputIdx] = itemsAssociated[nextI];
        }

        outputIdx++;
        if (outputIdx != valuesToDeduplicate.Length)
        {
            valuesToDeduplicate[outputIdx] = valuesToDeduplicate[^1];
            itemsAssociated[outputIdx] = itemsAssociated[^1];
        }

        return outputIdx;
    }
}
