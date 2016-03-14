using System;
using System.Diagnostics;
using System.Net.Http;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.OAuth;
using Raven.Client.Document;
using Raven.Json.Linq;

namespace Tryouts
{
    public class Program
    {
        public static void Main(string[] args)
        {
            using (var store = new DocumentStore
            {
                DefaultDatabase = "test",
                ApiKey = "user/abc",
                Url = "http://localhost:8081/"
            })
            {
                store.Initialize();

                var securedAuthenticator = new SecuredAuthenticator();
                // securedAuthenticator.DoOAuthRequest("http://live-test.ravendb.net/OAuth/API-Key", store.ApiKey);

                var sp = Stopwatch.StartNew();
                DoOperation(securedAuthenticator, store, 10);
                Console.WriteLine("After warmup {0:#,#;;0}", sp.ElapsedMilliseconds);
                while (true)
                {

                    sp.Reset();
                    sp.Start();
                    DoOperation(securedAuthenticator, store, 1, false);
                    Console.WriteLine("Total {0:#,#;;0}", sp.ElapsedMilliseconds);
                    Console.ReadKey();
                }

            }
        }

        private static void DoOperation(SecuredAuthenticator securedAuthenticator, DocumentStore store, int count, bool usesp = false)
        {

            // DoOp().Wait();
            for (int i = 0; i < count; i++)
            {
                try
                {
                    securedAuthenticator.DoOAuthRequestAsync("http://127.0.0.1:8081/oauth/api-key",
                        store.ApiKey,
                        usesp).Wait();

                    //var sp = Stopwatch.StartNew();
                    //DoGets(store, 100);
                    //Console.WriteLine("DoGets = " + sp.ElapsedMilliseconds);

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    Console.ReadKey();
                    return;
                }
            }
        }

        private static void DoGets(DocumentStore store, int i)
        {
            for (int j = 0; j < i; j++)
            {
            }
        }

        private static async Task DoOp()
        {
            while (true)
            {
                using (var webSocket = new ClientWebSocket())
                {
                    var sp = Stopwatch.StartNew();
                    await webSocket.ConnectAsync(new Uri("ws://127.0.0.1:8080/echo"), CancellationToken.None);
                    Console.WriteLine(sp.ElapsedMilliseconds);

                    var buffer = new byte[1024];
                    var helloWorld = "Hello World";
                    var size = Encoding.UTF8.GetBytes(helloWorld, 0, helloWorld.Length, buffer, 0);
                    await webSocket.SendAsync(new ArraySegment<byte>(buffer, 0, size), WebSocketMessageType.Text, true,
                        CancellationToken.None);
                    var webSocketReceiveResult =
                        await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
                    if (webSocketReceiveResult.Count != helloWorld.Length)
                    {
                        Console.WriteLine("???");
                    }
                    await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Thanks", CancellationToken.None);
                }
            }
        }
    }
}

//[RavenAction("/echo", "GET", "/oauth/api-key")]
//public async Task Echo()
//{
//    var sp = Stopwatch.StartNew();
//    using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
//    {
//        var buffer = new byte[1024];

//        while (true)
//        {
//            var result = await webSocket.ReceiveAsync(new ArraySegment<byte>(buffer), CancellationToken.None);
//            if (result.MessageType == WebSocketMessageType.Close)
//            {
//                webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Welcome", CancellationToken.None);
//                break;
//            }
//            await webSocket.SendAsync(new ArraySegment<byte>(buffer, 0, result.Count), WebSocketMessageType.Text, true,
//                CancellationToken.None);
//        }
//    }
//    Console.WriteLine(sp.ElapsedMilliseconds);
//}

#if false
// TEMP ADIADI TODO: remove this - we need route for this

TransactionOperationContext context2;
            using (ServerStore.ContextPool.AllocateOperationContext(out context2))
            {
                var tx = context2.OpenWriteTransaction();


var document = new DynamicJsonValue
{
    ["Enabled"] = "True",
    ["Secret"] = "secret",
    ["AccessMode"] = new DynamicJsonValue
    {
        ["dbname"] = "ReadWrite"
    }
};

var doc = context2.ReadObject(document, string.Empty);


ServerStore.Write(context2, $"{Constants.ApiKeyPrefix}{apiKeyName}", doc);
                tx.Commit();
            }
#endif


