using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Json;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using FastTests;
using StressTests;
using Voron;

namespace Tryouts
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (int i = 0; i < 100; i++)
            {
                Console.Write(i);
                var sp = Stopwatch.StartNew();
                using (var x = new FastTests.Client.Documents.BasicDocuments())
                {
                    x.GetAsyncWithTransformer().Wait();
                }
                Console.WriteLine(" - " + sp.Elapsed);
            }
        }
    }

}

