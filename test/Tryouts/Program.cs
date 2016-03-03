using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
using FastTests.Server.Documents.Notifications;
using FastTests.Voron.Bugs;
using FastTests.Voron.Trees;
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
        const string data = "{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/30\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":31}}Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/1\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":2}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/2\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":3}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/3\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":4}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/4\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":5}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/5\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":6}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/6\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":7}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/7\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":8}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/8\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":9}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/9\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":10}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/10\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":11}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/11\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":12}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/12\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":13}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/13\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":14}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/14\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":15}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/15\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":16}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/16\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":17}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/17\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":18}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/18\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":19}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/19\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":20}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/20\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":21}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/21\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":22}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/22\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":23}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/23\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":24}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/24\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":25}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/25\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":26}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/26\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":27}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/27\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":28}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/28\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":29}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/29\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":30}}{\"Type\":\"DocumentChangeNotification\",\"Value\":{\"Type\":1,\"Key\":\"users\\/30\",\"CollectionName\":\"Users\",\"TypeName\":null,\"Etag\":31}}";

        public static void Main(string[] args)
        {
            var basicIndexing = new FastTests.Server.Documents.Indexing.BasicIndexing();
            basicIndexing.SimpleIndexing();
        }
    }
}
