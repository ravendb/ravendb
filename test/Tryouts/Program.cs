using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
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
            var client = new ClientWebSocket();
            client.ConnectAsync(new Uri("ws://localhost.fiddler:8080/databases/test/changes"), CancellationToken.None).Wait();
            client.SendAsync(new ArraySegment<byte>(new byte[] {1, 2}), WebSocketMessageType.Text, true, CancellationToken.None);
        }
    }

    public class F : IWebProxy
    {
        public Uri GetProxy(Uri destination)
        {
            throw new NotImplementedException();
        }

        public bool IsBypassed(Uri host)
        {
            throw new NotImplementedException();
        }

        public ICredentials Credentials { get; set; }
    }
}
