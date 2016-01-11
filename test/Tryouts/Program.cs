using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using BlittableTests.Benchmark;
using BlittableTests.BlittableJsonWriterTests;
using NewBlittable;
using Newtonsoft.Json;
using Raven.Server.Json;

namespace Tryouts
{
    public unsafe class Program
    {
        public static void Main(string[] args)
        {
            foreach (var sample in UnmanageJsonReaderTests.Samples())
            {
                var f = (string) sample[0];
                //if (f.Contains("escape-str") == false) continue;
                Console.WriteLine(f);
                try
                {
                    new BlittableFormatTests().CheckRoundtrip(f);
                }
                catch (Exception e)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ResetColor();
                }
            }
        }
    }

}
