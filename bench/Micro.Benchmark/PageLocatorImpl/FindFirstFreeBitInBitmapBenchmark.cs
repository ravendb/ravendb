using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Sparrow.Binary;
using Sparrow.Server;
using Sparrow.Threading;
using Voron.Data.Tables;

namespace Micro.Benchmark.PageLocatorImpl;

[Config(typeof(Config))]
public unsafe class FindFirstFreeBitInBitmapBenchmark
{
    private class Config : ManualConfig
    {
        public Config()
        {
            AddJob(Job.Default
                .WithWarmupCount(100)
                .WithIterationCount(200)
                .WithInvocationCount(40000)
            );
        }
    }

    private readonly int _bitmapsCount = 1024;
    private ByteStringContext _context;
    private ulong* _workingBuffer; 
    
    [GlobalSetup]
    public void GlobalSetup()
    {
        _context = new ByteStringContext(SharedMultipleUseFlag.None);
        _context.Allocate(_bitmapsCount * sizeof(ulong) * 4, out ByteString workingBufferPtr);
        _workingBuffer = (ulong*)workingBufferPtr.Ptr;
        for (int i = 0; i < _bitmapsCount; ++i)
        {
            int j = 0;
            var k = i % 256;
            do
            {
                PtrBitVector.SetBitInPointer(_workingBuffer + k * 4, j++, true);
            } while (j < k);
        }
    }

    [GlobalCleanup]
    public void GlobalCleanup()
    {
        _context.Dispose();
    }

    [Benchmark(Baseline = true)]
    public long Naive()
    {
        long sumOfOffset = 0;
        for (int i = 0; i < _bitmapsCount; ++i)
        {
            sumOfOffset += FindFirstUnsetBitInBitmapNaive(_workingBuffer + i * 4);
        }

        return sumOfOffset;
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe int FindFirstUnsetBitInBitmapNaive(ulong* buffer)
    {
        for (int i = 0; i < 4; i++)
        {
            if (buffer[i] == ulong.MaxValue)
                continue;
                
            var currentBitOffset = 64 * i;

            for (int j = 0; j < 64; ++j)
            {
                if (PtrBitVector.GetBitInPointer(buffer, currentBitOffset + j) == false)
                    return currentBitOffset + j;
            }
        }

        return -1;
    }
    
    [Benchmark]
    public long LeadingZeros()
    {
        long sumOfOffset = 0;
        for (int i = 0; i < _bitmapsCount; ++i)
        {
            NewPageAllocator.TryFindFirstFreeBitInBitmap(_workingBuffer + i * 4, out var offset);
            sumOfOffset += offset;
        }

        return sumOfOffset;
    }
}
