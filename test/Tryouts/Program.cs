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
            var f = new BlittableFormatTests();
            foreach (var sample in BlittableFormatTests.Samples())
            {
                var s = (string)sample[0];

                Console.WriteLine(s);
                try
                {
                    f.CheckRoundtrip(s);
                }
                catch (Exception e)
                {
                    Console.ForegroundColor=ConsoleColor.Red;
                    Console.WriteLine(e);
                    Console.ResetColor();
                    break;
                }
            }

        }
    }

}
