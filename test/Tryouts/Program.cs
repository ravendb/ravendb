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
using FastTests.Server.Documents.Indexing.Auto;

namespace Tryouts
{
  
    public class Program
    {
        static void Main(string[] args)
        {
            using (var x = new BasicAutoMapReduceIndexing())
            {
                x.CanDelete().Wait();
            }
        }
    }
}

