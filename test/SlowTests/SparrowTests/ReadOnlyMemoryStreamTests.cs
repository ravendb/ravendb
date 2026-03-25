using System;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests;
using SlowTests.Utils;
using Sparrow.Server.Utils;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.SparrowTests;

public class ReadOnlyMemoryStreamTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    [RavenTheory(RavenTestCategory.Memory)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void CanReadFromReadOnlyMemoryStream(int seed)
    {
        var random = new Random(seed);
        var bufferLen = random.Next(1, 1024);
        var memorySource = new ReadOnlyMemory<float>(Enumerable.Range(0, bufferLen).Select(_ => random.NextSingle()).ToArray());
        var stream = new ReadOnlyMemoryStream<float>(memorySource);
        var buffer = new byte[(bufferLen + 30) * sizeof(float)];
        var bufferPos = 0;

        while (stream.Read(buffer, bufferPos, random.Next(1, 64)) is var read and > 0)
            bufferPos += read;

        var sourceMemory = MemoryMarshal.Cast<float, byte>(memorySource.Span);
        var destinationMemory = buffer.AsSpan(0, bufferPos);
        Assert.Equal(sourceMemory.Length, destinationMemory.Length);
        Assert.Equal(sourceMemory, destinationMemory);
    }
    
    [RavenTheory(RavenTestCategory.Memory)]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    [InlineDataWithRandomSeed]
    public void CanReadFromReadOnlyMemoryStreamWithLimitedBytes(int seed)
    {
        var random = new Random(seed);
        var bufferLen = random.Next(1, 1024);
        var memorySource = new ReadOnlyMemory<float>(Enumerable.Range(0, bufferLen).Select(_ => random.NextSingle()).ToArray());
        var sizeOfMemorySource = memorySource.Length * sizeof(float);
        var toRead = random.Next(1, sizeOfMemorySource);
        
        
        var stream = new ReadOnlyMemoryStream<float>(memorySource, toRead);
        var buffer = new byte[(bufferLen + 30) * sizeof(float)];
        var bufferPos = 0;

        while (stream.Read(buffer, bufferPos, random.Next(1, 64)) is var read and > 0)
            bufferPos += read;

        var sourceMemory = MemoryMarshal.Cast<float, byte>(memorySource.Span)[..toRead];
        var destinationMemory = buffer.AsSpan(0, bufferPos);
        Assert.Equal(stream.Position, stream.Length);
        Assert.Equal(sourceMemory.Length, destinationMemory.Length);
        Assert.Equal(sourceMemory, destinationMemory);
    }
}
