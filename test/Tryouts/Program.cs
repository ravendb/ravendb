using System;
using System.Linq;
using System.Net.WebSockets;
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Client.BulkInsert;
using FastTests.Server.Documents.Indexing;
using FastTests.Voron.RawData;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Client.Platform;

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
            for (int i = 0; i < 100; i++)
            {
                Console.WriteLine(i);
                using (var x = new BasicIndexing())
                {
                    x.Errors();
                }
            }
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
