using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using BlittableTests;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Server.Json;
using Sparrow;
using Voron;
using Voron.Util;

namespace Tryouts
{
    public class Program
    {


        public unsafe static void Main(string[] args)
        {
            var s = "1.233     ";
            var d = double.Parse(s,NumberStyles.Any);
            Console.WriteLine(d);
            return;
            //new SmallStringCompressionTests().RoundTrip(s: "See: here");


            //Console.WriteLine("start");
            //var blittableFormatTests = new UnmanagedStreamTests();
            //blittableFormatTests.BigAlloc();
            //GC.Collect(2);
            //GC.WaitForPendingFinalizers();
            //Console.WriteLine("Done");
            //return;

            //force loading of assemblyes
            Console.WriteLine(typeof(UnmanageJsonReaderTests));
            Console.WriteLine(typeof(BlittableJsonWriter));
            Console.WriteLine(typeof(Hashing));
            Console.WriteLine(typeof(StorageEnvironment));

            WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", 2);
            Console.WriteLine("Really starting now...");
            WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", int.MaxValue);

            WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines", 2);
            Console.WriteLine("Really starting now...");
            WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines", int.MaxValue);
            Console.WriteLine("done!");
            Console.ReadLine();
        }
    }
}