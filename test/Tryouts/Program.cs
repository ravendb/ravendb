using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using BlittableTests;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server.Json;
using Sparrow;
using Voron;

namespace Tryouts
{
    public class F
    {

        private static readonly int[] nextPowerOf2Table =
        {
              0,   1,   2,   4,   4,   8,   8,   8,   8,  16,  16,  16,  16,  16,  16,  16,
             16,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,
             32,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,
             64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,  64,
             64, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
            128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
            128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
            128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128,
            128, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256,
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256,
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256,
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256,
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256,
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256,
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256,
            256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256, 256
        };


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int NextPowerOf2(int v)
        {
            if (v < nextPowerOf2Table.Length)
            {
                return nextPowerOf2Table[v];
            }
            else
            {
                return NextPowerOf2Internal(v);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int NextPowerOf2Internal(int v)
        {
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v++;

            return v;
        }
    }
    public class Program
    {


        public static void Main(string[] args)
        {

            Console.WriteLine("start");
            var blittableFormatTests = new UnmanagedStreamTests();
            blittableFormatTests.BigAlloc();
            GC.Collect(2);
            GC.WaitForPendingFinalizers();
            Console.WriteLine("Done");
            return;

            ////force loading of assemblyes
            //Console.WriteLine(typeof(UnmanageJsonReaderTests));
            //Console.WriteLine(typeof(BlittableJsonWriter));
            //Console.WriteLine(typeof(Hashing));
            //Console.WriteLine(typeof(StorageEnvironment));

            //WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", 2);
            //Console.WriteLine("Reallying starting now...");
            //WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", int.MaxValue);

            //WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines");
            //Console.WriteLine("Reallying starting now...");
            //WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines");
            //Console.ReadLine();

        }
    }
}