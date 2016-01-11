using System;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            WriteToStreamBenchmark.PerformanceAnalysis(@"D:\json\Big", "output.csv", 2);
            Console.WriteLine("Reallying starting now...");
            WriteToStreamBenchmark.PerformanceAnalysis(@"D:\json\Big", "output.csv", int.MaxValue);

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
        }
    }
}