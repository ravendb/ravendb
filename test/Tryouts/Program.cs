using System;
using System.IO;
using System.IO.Compression;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
<<<<<<< HEAD
using FastTests.Client.BulkInsert;
using FastTests.Server.Documents.Indexing;
using FastTests.Server.Documents.Queries.Dynamic.MapReduce;
using FastTests.Voron.RawData;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
=======

>>>>>>> c28a72ed531a8373a40873155dae8a363c13987f
using Raven.Abstractions.Data;
using Raven.Abstractions.Json;
using Raven.Client.Document;
using Raven.Client.Platform;
using Raven.Json.Linq;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Static;

using JsonToken = Raven.Imports.Newtonsoft.Json.JsonToken;

namespace Tryouts
{
    public class Program
    {
        public class User
        {
            public string FirstName { get; set; }

            public string LastName { get; set; }

            public string[] Tags { get; set; }
        }

        public static void Main(string[] args)
        {

            //             MetadataReference[] References =
            //            {
            //                MetadataReference.CreateFromFile(typeof (object).GetTypeInfo().Assembly.Location),
            //                MetadataReference.CreateFromFile(typeof (Enumerable).GetTypeInfo().Assembly.Location),
            //                MetadataReference.CreateFromFile(typeof (DynamicAttribute).GetTypeInfo().Assembly.Location),
            //                MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("System.Runtime")).Location),
            //                MetadataReference.CreateFromFile(Assembly.Load(new AssemblyName("Microsoft.CSharp")).Location),
            //            };

            //            SyntaxTree syntaxTree = CSharpSyntaxTree.ParseText(@"
            //public class MyClass
            //{
            //    public void Method(dynamic a)
            //    {
            //        a.Run();
            //    }
            //}
            //");
            //            CSharpCompilation compilation = CSharpCompilation.Create(
            //                 assemblyName: "test.dll",
            //                 syntaxTrees: new[] { syntaxTree },
            //                 references: References,
            //                 options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary)
            //                 );
            //            var emitResult = compilation.Emit(new MemoryStream());
            //            foreach (var diagnostic in emitResult.Diagnostics)
            //            {
            //                Console.WriteLine(diagnostic.ToString());
            //            }
            //            return;

<<<<<<< HEAD
            new BasicDynamicMapReduceQueries().Can_project_in_map_reduce().Wait();
=======
            var indexDefinition = new IndexDefinition
            {
                Name = "Orders_ByName",
                Maps =
                {
                    "from order in docs.Orders select new { Mame = order.Name.ToUpper() };"
                }
            };

            var index = StaticIndexCompiler.Compile(indexDefinition);

            var orders = new[]
            {
                new Order {Name = "Oren"},
                new Order {Name = "Pawel"},
                new Order {Name = "Haim"},
            };

            foreach (var collectionMaps in index.Maps)
            {
                foreach (var map in collectionMaps.Value)
                {
                    foreach (var result in map(orders))
                    {
                        Console.WriteLine(result);
                    }
                }
            }
>>>>>>> c28a72ed531a8373a40873155dae8a363c13987f
        }

        public class Order
        {
            public string Name;
        }

        private static void createNewDB(DocumentStore store)
        {
            store.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
            {
                Id = "BenchmarkDB",
                Settings =
                {
                    {"Raven/DataDir", "~/BenchmarkDB"},
                    {"Raven/ActiveBundles", ""}
                }
            });
        }

        private static async Task importData(DocumentStore store)
        {
            //using (var bulk = store.BulkInsert())
            {

                string filePath = @"C:\Users\ayende\Downloads\Dump of temp2, 2016-05-17 14-07.ravendump";
                Stream dumpStream = File.OpenRead(filePath);
                var gZipStream = new GZipStream(dumpStream, CompressionMode.Decompress, leaveOpen: true);
                using (var streamReader = new StreamReader(gZipStream))
                using (var reader = new RavenJsonTextReader(streamReader))
                {

                    if (reader.Read() == false /* { */|| reader.Read() == false /* prop*/)
                        throw new InvalidOperationException("empty document?");

                    if (reader.TokenType != JsonToken.PropertyName)
                        throw new InvalidOperationException("Expected property");

                    if ((string)reader.Value != "Docs")
                        throw new InvalidOperationException("Expected property name 'Docs'");

                    if (reader.Read() == false)
                        throw new InvalidOperationException("corrupt document");

                    if (reader.TokenType != JsonToken.StartArray)
                        throw new InvalidOperationException("corrupt document, missing array");

                    if (reader.Read() == false)
                        throw new InvalidOperationException("corrupt document, array value");

                    while (reader.TokenType != JsonToken.EndArray)
                    {
                        var document = RavenJObject.Load(reader);
                        var metadata = document.Value<RavenJObject>("@metadata");
                        var key = metadata.Value<string>("@id");
                        document.Remove("@metadata");


                        await store.AsyncDatabaseCommands.PutAsync(key, null, document, metadata);
                        //await bulk.StoreAsync(document, metadata, key).ConfigureAwait(false);
                        Console.WriteLine(key);
                        if (reader.Read() == false)
                            throw new InvalidOperationException("corrupt document, array value");
                    }
                }
            }
        }

        public class ContactClass
        {
            public string Name { get; set; }
            public string Title { get; set; }
        }
        public class AddressClass
        {
            public string Line1 { get; set; }
            public object Line2 { get; set; }
            public string City { get; set; }
            public object Region { get; set; }
            public int PostalCode { get; set; }
            public string Country { get; set; }
        }
        public class Company
        {
            public string ExternalId { get; set; }
            public string Name { get; set; }
            public ContactClass Contact { get; set; }
            public AddressClass Address { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
        }

        private static async Task DoWork()
        {
            using (var ws = new RavenClientWebSocket())
            {
                await ws.ConnectAsync(new Uri("ws://echo.websocket.org"), CancellationToken.None);

                await
                    ws.SendAsync(new ArraySegment<byte>(Encoding.UTF8.GetBytes("Hello there")),
                        WebSocketMessageType.Text,
                        true, CancellationToken.None);

                var arraySegment = new ArraySegment<byte>(new byte[1024]);
                var webSocketReceiveResult = await ws.ReceiveAsync(arraySegment, CancellationToken.None);
                var s = Encoding.UTF8.GetString(arraySegment.Array, 0, webSocketReceiveResult.Count);
                Console.WriteLine();
                Console.WriteLine(s);
            }
        }
        public static async Task BulkInsert(DocumentStore store, int numOfItems)
        {
            Console.Write("Doing bulk-insert...");

            string[] tags = null;// Enumerable.Range(0, 1024*8).Select(x => "Tags i" + x).ToArray();

            var sp = System.Diagnostics.Stopwatch.StartNew();
            using (var bulkInsert = store.BulkInsert())
            {
                int id = 1;
                for (int i = 0; i < numOfItems; i++)
                    await bulkInsert.StoreAsync(new User
                    {
                        FirstName = $"First Name - {i}",
                        LastName = $"Last Name - {i}",
                        Tags = tags
                    }, $"users/{id++}");
            }
            Console.WriteLine("done in " + sp.Elapsed);
        }
    }
}
