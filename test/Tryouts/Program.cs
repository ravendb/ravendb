using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using FastTests.Blittable;
using FastTests.Blittable.Benchmark;
using FastTests.Blittable.BlittableJsonWriterTests;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Indexing;
using FastTests.Voron.Bugs;
using Newtonsoft.Json;
using Raven.Client.Document;
using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Analyzers;
using Raven.Server.Json;
using Raven.Server.Json.Parsing;
using Raven.Tests.Core;
using Tryouts.Corax;
using Tryouts.Corax.Tests;
using Voron;
using Voron.Debugging;
using Xunit;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var x = new DocumentsCrud())
            {
                x.CanDelete("users/1");
            }
        }
    }
}
