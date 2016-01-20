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
using BlittableTests.Routing;
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
            new TrieTests().CanQueryTrie();

            ////Console.WriteLine("start");
            ////var blittableFormatTests = new UnmanagedStreamTests();
            ////blittableFormatTests.BigAlloc();
            ////GC.Collect(2);
            ////GC.WaitForPendingFinalizers();
            ////Console.WriteLine("Done");
            ////return;

            ////force loading of assemblyes
            //Console.WriteLine(typeof(UnmanageJsonReaderTests));
            //Console.WriteLine(typeof(BlittableJsonDocument));
            //Console.WriteLine(typeof(Hashing));
            //Console.WriteLine(typeof(StorageEnvironment));

            ////WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", 2);
            ////Console.WriteLine("Really starting now...");
            ////WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", int.MaxValue);

            ////WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines", 2);
            //Console.WriteLine("Really starting now...");
            //WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines", int.MaxValue);
            //Console.WriteLine("done!");
        }
    }
}