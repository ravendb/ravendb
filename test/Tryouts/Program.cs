using System;
using System.Diagnostics;
using System.IO;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", 2);
            Console.WriteLine("Reallying starting now...");
            WriteToStreamBenchmark.PerformanceAnalysis(@"C:\Work\JSON\Big", "output.csv", int.MaxValue);

            WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines");
            Console.WriteLine("Reallying starting now...");
            WriteToStreamBenchmark.ManySmallDocs(@"C:\Work\JSON\Lines");

            //new FunctionalityTests().LongStringsTest(1000);

            //foreach (var sample in UnmanageJsonReaderTests.Samples())
            //{
            //    var f = (string) sample[0];
            //    if (f.Contains("escape-str") == false) continue;
            //    Console.WriteLine(f);
            //    try
            //    {
            //        new BlittableFormatTests().CheckRoundtrip(f);
            //    }
            //    catch (Exception e)
            //    {
            //        Console.ForegroundColor = ConsoleColor.Red;
            //        Console.WriteLine(e);
            //        Console.ResetColor();
            //    }
            //}

            Console.WriteLine();

            Console.WriteLine("{0:#,#}", Process.GetCurrentProcess().PeakWorkingSet64 / 1024);
            Console.ReadLine();
        }
    }
}