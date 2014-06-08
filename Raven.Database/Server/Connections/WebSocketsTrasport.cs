using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Connections
{
    /*
     * This is really ugly way to go about it, but that is the interface that OWIN
     * gives us
     * http://owin.org/extensions/owin-WebSocket-Extension-v0.4.0.htm
     * 
    */
    using WebSocketAccept = Action<IDictionary<string, object>, // options
        Func<IDictionary<string, object>, Task>>; // callback
    using WebSocketCloseAsync =
        Func<int /* closeStatus */,
            string /* closeDescription */,
            CancellationToken /* cancel */,
            Task>;
    using WebSocketReceiveAsync =
        Func<ArraySegment<byte> /* data */,
            CancellationToken /* cancel */,
            Task<Tuple<int /* messageType */,
                bool /* endOfMessage */,
                int /* count */>>>;
    using WebSocketSendAsync =
        Func<ArraySegment<byte> /* data */,
            int /* messageType */,
            bool /* endOfMessage */,
            CancellationToken /* cancel */,
            Task>;
    using WebSocketReceiveResult = Tuple<int, // type
        bool, // end of message?
        int>; // count

    public class WebSocketsTrasport : IEventsTransport
    {
        private readonly IOwinContext _context;
        private readonly RavenDBOptions _options;


        private readonly AsyncManualResetEvent manualResetEvent = new AsyncManualResetEvent();

        private readonly ConcurrentQueue<object> msgs = new ConcurrentQueue<object>();

        public WebSocketsTrasport(RavenDBOptions options, IOwinContext context)
        {
            _options = options;
            _context = context;
            Connected = true;
            Id = context.Request.Query["id"];
        }

        public void Dispose()
        {
        }

        public string Id { get; private set; }
        public bool Connected { get; set; }

        public event Action Disconnected;

        public void SendAsync(object msg)
        {
            msgs.Enqueue(msg);
            manualResetEvent.Set();
        }

        public async Task Run(IDictionary<string, object> websocketContext)
        {
            try
            {
                var sendAsync = (WebSocketSendAsync) websocketContext["websocket.SendAsync"];
                //var receiveAsync = (WebSocketReceiveAsync)websocketContext["websocket.ReceiveAsync"];
                var closeAsync = (WebSocketCloseAsync) websocketContext["websocket.CloseAsync"];
                var callCancelled = (CancellationToken) websocketContext["websocket.CallCancelled"];

                var memoryStream = new MemoryStream();
                var serializer = new JsonSerializer
                {
                    Converters = {new EtagJsonConverter()}
                };
                while (callCancelled.IsCancellationRequested == false)
                {
                    bool result = await manualResetEvent.WaitAsync(5000);
                    if (callCancelled.IsCancellationRequested)
                        return;

                    if (result == false)
                    {
                        await SendMessage(memoryStream, serializer,
                            new { Type = "Heartbeat", Time = SystemTime.UtcNow },
                            sendAsync, callCancelled);
                        continue;
                    }
                    manualResetEvent.Reset();
                    object message;
                    while (msgs.TryDequeue(out message))
                    {
                        if (callCancelled.IsCancellationRequested)
                            break;

                        await SendMessage(memoryStream, serializer, message, sendAsync, callCancelled);
                    }
                }
                try
                {
                    await closeAsync((int) websocketContext["websocket.ClientCloseStatus"], (string) websocketContext["websocket.ClientCloseDescription"], callCancelled);
                }
                catch (Exception)
                {
                }
            }
            finally
            {
                OnDisconnection();
            }
        }

        private static async Task SendMessage(MemoryStream memoryStream, JsonSerializer serializer, object message, WebSocketSendAsync sendAsync, CancellationToken callCancelled)
        {
            memoryStream.Position = 0;
            var jsonTextWriter = new JsonTextWriter(new StreamWriter(memoryStream));
            serializer.Serialize(jsonTextWriter, message);
            jsonTextWriter.Flush();

            var arraySegment = new ArraySegment<byte>(memoryStream.GetBuffer(), 0, (int) memoryStream.Position);
            await sendAsync(arraySegment, 1, true, callCancelled);
        }

        private void OnDisconnection()
        {
            Connected = false;
            Action onDisconnected = Disconnected;
            if (onDisconnected != null)
                onDisconnected();
        }

        public async Task<bool> TrySetupRequest()
        {
            if (string.IsNullOrEmpty(Id))
            {
                _context.Response.StatusCode = 400;
                _context.Response.ReasonPhrase = "BadRequest";
                _context.Response.Write("Id is mandatory");
                return false;
            }
            var dbName = GetDatabaseName();

            DocumentDatabase documentDatabase;
            try
            {
                documentDatabase = await _options.DatabaseLandlord.GetDatabaseInternal(dbName);
            }
            catch (Exception e)
            {
                _context.Response.StatusCode = 500;
                _context.Response.ReasonPhrase = "InternalServerError";
                _context.Response.Write(e.ToString());
                return false;
            }

            // TODO: check permissions

            documentDatabase.TransportState.Register(this);

            return true;
        }

        private string GetDatabaseName()
        {
            var localPath = _context.Request.Uri.LocalPath;
            const string databasesPrefix = "/databases/";
            if (localPath.StartsWith(databasesPrefix) == false)
                return null;

            var indexOf = localPath.IndexOf('/', databasesPrefix.Length+1);
            if (indexOf == -1)
                return null;
            return localPath.Substring(databasesPrefix.Length, indexOf - databasesPrefix.Length);
        }
    }
}