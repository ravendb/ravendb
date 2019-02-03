using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Sparrow;

namespace Tests.Infrastructure.InterversionTest
{
    public static class WebSocketUtil
    {
        public static async Task<string> ReadFromWebSocket(ArraySegment<byte> buffer, WebSocket source, CancellationToken ct)
        {
            using (var ms = new MemoryStream())
            {
                WebSocketReceiveResult result;
                do
                {
                    try
                    {
                        result = await source.ReceiveAsync(buffer, ct);
                    }
                    catch (Exception)
                    {
                        break;
                    }
                    ms.Write(buffer.Array, buffer.Offset, result.Count);
                }
                while (!result.EndOfMessage);
                ms.Seek(0, SeekOrigin.Begin);

                return new StreamReader(ms, Encoding.UTF8).ReadToEnd();
            }
        }

        public static async Task<string> CollectAdminLogs(IDocumentStore store, CancellationToken ct)
        {
            var str = string.Format("{0}/admin/logs/watch", store.Urls.First().Replace("http", "ws"));
            var tempFileName = Path.GetTempFileName();
            using(var writer = File.CreateText(tempFileName))
            using (var client = new ClientWebSocket())
            {
                await client.ConnectAsync(new Uri(str), CancellationToken.None);
                ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024]);
                while (client.State == WebSocketState.Open)
                {
                    if (ct.IsCancellationRequested)
                    {
                        break;
                    }

                    await writer.WriteLineAsync(await ReadFromWebSocket(buffer, client, ct));
                }
            }

            return tempFileName;
        }
    }
}
