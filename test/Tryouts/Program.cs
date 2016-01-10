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
            WriteToStreamBenchmark.Indexing(@"C:\Work\JSON\Lines");
        }
    }

}
