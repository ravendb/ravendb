using System;
using Corax;
using FastTests;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Voron.Graphs;

public class HnswVectorStorage(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void MaximumNumberOfVectorInSelfManagedContainerIsNotExceedingByteMax()
    {
        var maxVectorCountInSingleContainer = 1;
        for (long vectorSize = 1; vectorSize <= int.MaxValue; vectorSize++)
        {
            Hnsw.Options options = new() { VectorSizeBytes = (int)vectorSize };
            
            if (options.VectorBatchInPages is 1)
                continue;
            
            var freeSpace = options.VectorBatchInPages * global::Voron.Global.Constants.Storage.PageSize - PageHeader.SizeOf;
            var countOfVectorsInContainer = freeSpace / vectorSize;
            
            maxVectorCountInSingleContainer = Math.Max(maxVectorCountInSingleContainer, (int)countOfVectorsInContainer);
        }
        
        Assert.True(maxVectorCountInSingleContainer <= byte.MaxValue);
        Assert.Equal(42, maxVectorCountInSingleContainer); //current max
    }
}
