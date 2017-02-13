using Raven.Client.Connection;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Voron;
using Voron;
using Voron.Global;
using Voron.Impl.Scratch;
using Xunit;
using Sparrow.Platform;
using System.Linq;
using System.Runtime.Serialization.Formatters;
using System.Text;
using FastTests.Blittable;
using FastTests.Issues;
using FastTests.Server.Basic;
using Raven.Client.Json;
using FastTests.Server.Documents;
using FastTests.Server.Documents.Expiration;
using FastTests.Server.Documents.Queries;
using FastTests.Server.Replication;
using FastTests.Voron.FixedSize;
using FastTests.Voron.RawData;
using FastTests.Voron.Tables;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;
using Raven.Server.Config;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using SlowTests.Server.Documents.SqlReplication;
using SlowTests.Tests;
using SlowTests.Voron;
using Sparrow.Json;
using Sparrow.Logging;
using Voron.Data.Tables;
using Raven.Client.Document;

namespace Tryouts
{
    public class Program
    {
        public class Item
        {
            [JsonExtensionData]
            public IDictionary<string, JToken> ExtensionData;

            public IEnumerable<string> Foo
            {
                get { yield return "foo"; yield return "bar"; }
            }
        }

        public static void Main(string[] args)
        {
            for (int i = 0; i < 1000; i++)
            {
                Console.WriteLine(i);
                using (var a = new SlowTests.Issues.RavenDB_4011())
                {
                    a.get_index_names();
                }
            }
        }
    }
}