using BenchmarkDotNet;
using BenchmarkDotNet.Tasks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sparrow.Tryout
{
    public static class Performance
    {

        [BenchmarkTask(platform: BenchmarkPlatform.X86)]
        [BenchmarkTask(platform: BenchmarkPlatform.X64)]
        public class HashingPerf
        {
            private const int N = 30;
            private readonly byte[] data;

            public HashingPerf()
            {
                data = new byte[N];
                new Random(42).NextBytes(data);
            }

            [Benchmark]
            public uint XXHash32()
            {
                unsafe
                {
                    int dataLength = data.Length;
                    fixed (byte* dataPtr = data)
                    {
                        return Hashing.XXHash32.CalculateInline(dataPtr, dataLength);
                    }
                }
            }

            [Benchmark]
            public ulong XXHash64()
            {
                unsafe
                {
                    int dataLength = data.Length;
                    fixed (byte* dataPtr = data)
                    {
                        return Hashing.XXHash64.CalculateInline(dataPtr, dataLength);
                    }
                }
            }
        }




        public static void Main(string[] args)
        {
            new BenchmarkCompetitionSwitch(new[] { typeof(HashingPerf) }).Run(args);
        }
    }
}
