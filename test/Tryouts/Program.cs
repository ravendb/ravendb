using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using AsyncFriendlyStackTrace;
using Raven.Abstractions.Extensions;
using Raven.Client.Document;
using Raven.Client.Smuggler;
using Raven.Server.ServerWide.Context;
using Raven.SlowTests.Issues;
using Sparrow.Json;

namespace Tryouts
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var x = new FastTests.Server.Documents.Indexing.Static.CollisionsOfReduceKeyHashes();
            x.Static_index_should_produce_multiple_outputs(4, new[] {"Israel", "Poland"}).Wait();
        }
    }
}