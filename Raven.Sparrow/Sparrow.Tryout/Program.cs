using BenchmarkDotNet;
using BenchmarkDotNet.Tasks;
using Sparrow;
using System;

namespace Sparrow.Tryout
{

    [Task(platform: BenchmarkPlatform.X64, jitVersion: BenchmarkJitVersion.LegacyJit)]
    [Task(platform: BenchmarkPlatform.X64, jitVersion: BenchmarkJitVersion.RyuJit)]
    public class HashingBenchmark
    {
        public byte[] buffer = new byte[4096 * 4096];

        public HashingBenchmark()
        {
            Random generator = new Random();
            generator.NextBytes(buffer);
        }

        [Benchmark]
        public uint HashHuge()
        {
            unsafe
            {
                fixed (byte* bufferPtr = buffer)
                {
                    return Hashing.XXHash32.CalculateInline(bufferPtr, buffer.Length);
                }
            }
        }

        [Benchmark]
        public uint HashBlock()
        {
            unsafe
            {
                fixed (byte* bufferPtr = buffer)
                {
                    return Hashing.XXHash32.CalculateInline(bufferPtr, 512);
                }
            }
        }

        [Benchmark]
        public uint HashSmall()
        {
            unsafe
            {
                fixed (byte* bufferPtr = buffer)
                {
                    return Hashing.XXHash32.CalculateInline(bufferPtr, 64);
                }
            }
        }

        [Benchmark]
        public uint HashVerySmall()
        {
            unsafe
            {
                fixed (byte* bufferPtr = buffer)
                {
                    return Hashing.XXHash32.CalculateInline(bufferPtr, 16);
                }
            }
        }

        [Benchmark]
        public uint HashTiny()
        {
            unsafe
            {
                fixed (byte* bufferPtr = buffer)
                {
                    return Hashing.XXHash32.CalculateInline(bufferPtr, 6);
                }
            }
        }
    }


    [Task(platform: BenchmarkPlatform.X64, jitVersion: BenchmarkJitVersion.LegacyJit)]
    [Task(platform: BenchmarkPlatform.X64, jitVersion: BenchmarkJitVersion.RyuJit)]
    public class MemoryCompareBenchmark
    {
        public byte[] bufferSrc = new byte[4096 * 4096];
        public byte[] bufferDest = new byte[4096 * 4096];

        public MemoryCompareBenchmark()
        {
            Random generator = new Random();
            generator.NextBytes(bufferSrc);

            for (int i = 0; i < bufferSrc.Length; i++)
                bufferDest[i] = bufferSrc[i];
        }

        [Benchmark]
        public void CompareHuge()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CompareInline(bufferSrcPtr, bufferDestPtr, bufferSrc.Length);
                }
            }
        }

        [Benchmark]
        public void CompareBlock()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CompareInline(bufferSrcPtr, bufferDestPtr, 4096);
                }
            }
        }

        [Benchmark]
        public void CompareSmall()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CompareInline(bufferSrcPtr, bufferDestPtr, 64);
                }
            }
        }

        [Benchmark]
        public void CompareVerySmall()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CompareInline(bufferSrcPtr, bufferDestPtr, 16);
                }
            }
        }

        [Benchmark]
        public void CompareTiny()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CompareInline(bufferSrcPtr, bufferDestPtr, 6);
                }
            }
        }
    }

    [Task(platform: BenchmarkPlatform.X64, jitVersion: BenchmarkJitVersion.LegacyJit)]
    [Task(platform: BenchmarkPlatform.X64, jitVersion: BenchmarkJitVersion.RyuJit)]
    public class MemoryCopyBenchmark
    {
        public byte[] bufferSrc = new byte[4096 * 4096];
        public byte[] bufferDest = new byte[4096 * 4096];

        [Benchmark]
        public void CopyHuge()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CopyInline(bufferDestPtr, bufferSrcPtr, bufferSrc.Length);
                }
            }
        }

        [Benchmark]
        public void CopyBlock()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CopyInline(bufferDestPtr, bufferSrcPtr, 512);
                }
            }
        }

        [Benchmark]
        public void CopySmall()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CopyInline(bufferDestPtr, bufferSrcPtr, 64);
                }
            }
        }

        [Benchmark]
        public void CopyVerySmall()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CopyInline(bufferDestPtr, bufferSrcPtr, 16);
                }
            }
        }

        [Benchmark]
        public void CopyTiny()
        {
            unsafe
            {
                fixed (byte* bufferSrcPtr = bufferSrc)
                fixed (byte* bufferDestPtr = bufferDest)
                {
                    Memory.CopyInline(bufferDestPtr, bufferSrcPtr, 6);
                }
            }
        }
    }


    public unsafe class Program
    {
        public static void Main(string[] args)
        {
            var competitionSwitch = new BenchmarkCompetitionSwitch(new[] {               
                typeof(MemoryCompareBenchmark),
                typeof(MemoryCopyBenchmark),
                typeof(HashingBenchmark)
            });
            competitionSwitch.Run(args);
        }
    }
}
