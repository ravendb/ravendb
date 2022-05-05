using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Corax;
using FastTests.Corax;
using FastTests.Sparrow;
using FastTests.Voron;
using FastTests.Voron.Sets;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Sparrow.Server.Compression;
using Sparrow.Threading;
using Tests.Infrastructure;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Sets;
using Raven.Server.Documents.Queries.Parser;
using Corax.Queries;
using NuGet.Packaging.Signing;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            //XunitLogging.RedirectStreams = false;
        }

        public static void Main()
        {
            new SimplePipelineTest(new ConsoleTestOutputHelper()).BasicAnalyzer();
        }
    }
}
