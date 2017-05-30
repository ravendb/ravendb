// -----------------------------------------------------------------------
//  <copyright file="TrafficRec.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Sparrow.Logging;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Traffic
{
    public class TrafficRec
    {
        private readonly IDocumentStore _store;
        private readonly TrafficToolConfiguration _config;
        private readonly JsonContextPool _jsonContextPool = new JsonContextPool();
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<TrafficRec>("Raven/Traffic");

        public TrafficRec(IDocumentStore store, TrafficToolConfiguration config)
        {
            _store = store;
            _config = config;
        }

        public void ExecuteTrafficCommand()
        {
            switch (_config.Mode)
            {
                case TrafficToolConfiguration.TrafficToolMode.Record:
                    RecordRequests(_config, _store).Wait();
                    break;
                case TrafficToolConfiguration.TrafficToolMode.Replay:
                    ReplayRequests(_config, _store);
                    break;
            }
        }

        private void ReplayRequests(TrafficToolConfiguration config, IDocumentStore store)
        {
            throw new NotImplementedException();
        }


        private async Task<BlittableJsonReaderObject> Receive(ClientWebSocket webSocket,
            JsonOperationContext context)
        {
            BlittableJsonDocumentBuilder builder = null;
            try
            {
                if (webSocket.State != WebSocketState.Open)
                    throw new InvalidOperationException(
                        $"Trying to 'ReceiveAsync' WebSocket while not in Open state. State is {webSocket.State}");

                var state = new JsonParserState();
                JsonOperationContext.ManagedPinnedBuffer buffer;
                using (context.GetManagedBuffer(out buffer))
                using (var parser = new UnmanagedJsonParser(context, state, "")) //TODO: FIXME
                {
                    builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.None,
                        nameof(TrafficRec) + "." + nameof(Receive), parser, state);
                    builder.ReadObjectDocument();
                    while (builder.Read() == false)
                    {
                        var result = await webSocket.ReceiveAsync(buffer.Buffer, CancellationToken.None);

                        if (result.MessageType == WebSocketMessageType.Close)
                        {
                            if (_logger.IsInfoEnabled)
                            {
                                _logger.Info("Client got close message from server and is closing connection");
                            }

                            builder.Dispose();
                            // actual socket close from dispose
                            return null;
                        }

                        if (result.EndOfMessage == false)
                        {
                            throw new EndOfStreamException("Stream ended without reaching end of json content.");
                        }

                        parser.SetBuffer(buffer, 0, result.Count);
                    }
                    builder.FinalizeDocument();

                    return builder.CreateReader();
                }
            }
            catch (WebSocketException ex)
            {
                builder?.Dispose();

                if (_logger.IsInfoEnabled)
                {
                    _logger.Info("Failed to receive a message, client was probably disconnected", ex);
                }

                throw;
            }
        }

        /// <summary>
        /// Connects to raven traffic event source and registers all the requests to the file defined in the config
        /// </summary>
        /// <param name="config">configuration conatining the connection, the file to write to, etc.</param>
        /// <param name="store">the store to work with</param>
        private async Task RecordRequests(TrafficToolConfiguration config, IDocumentStore store)
        {
            var id = Guid.NewGuid().ToString();
            using (var client = new ClientWebSocket())
            {
                var url = store.Urls.First() + "/traffic-watch/websockets";
                var uri = new Uri(url.ToWebSocketPath());

                await client.ConnectAsync(uri, CancellationToken.None)
                    .ConfigureAwait(false);


                // record traffic no more then 7 days
                var day = 24 * 60 * 60;
                var timeout = (int)config.Timeout.TotalMilliseconds / 1000;
                timeout = Math.Min(timeout, 7 * day);
                if (timeout <= 0)
                    timeout = 7 * day;

                try
                {
                    string resourceName = config.ResourceName ?? "N/A";
                    var connectMessage = new DynamicJsonValue
                    {
                        ["Id"] = id,
                        ["DatabaseName"] = resourceName,
                        ["Timeout"] = timeout
                    };

                    var stream = new MemoryStream();

                    JsonOperationContext context;
                    using (_jsonContextPool.AllocateOperationContext(out context))
                    using (var writer = new BlittableJsonTextWriter(context, stream))
                    {
                        context.Write(writer, connectMessage);
                        writer.Flush();

                        ArraySegment<byte> bytes;
                        stream.TryGetBuffer(out bytes);
                        await client.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None)
                            .ConfigureAwait(false);
                    }

                    var requestsCounter = 0;
                    using (var fileStream = File.Create(config.RecordFilePath))
                    {
                        Stream finalStream = fileStream;
                        if (config.IsCompressed)
                            finalStream = new GZipStream(fileStream, CompressionMode.Compress, leaveOpen: true);

                        using (var streamWriter = new StreamWriter(finalStream))
                        {
                            var jsonWriter = new JsonTextWriter(streamWriter)
                            {
                                Formatting = Formatting.Indented
                            };
                            jsonWriter.WriteStartArray();
                            var sp = Stopwatch.StartNew();

                            while (true)
                            {
                                using (var reader = await Receive(client, context))
                                {
                                    if (reader == null)
                                    {
                                        // server asked to close connection
                                        break;
                                    }

                                    string type;
                                    if (reader.TryGet("Type", out type))
                                    {
                                        if (type.Equals("Heartbeat"))
                                        {
                                            continue;
                                        }
                                    }

                                    string error;
                                    if (reader.TryGet("Error", out error))
                                    {
                                        throw new InvalidOperationException("Server returned error: " + error);
                                    }

                                    var notification = new TrafficWatchChange();
                                    notification.TimeStamp = GetDateTimeFromJson(reader, "TimeStamp");
                                    notification.RequestId = GetIntFromJson(reader, "RequestId");
                                    notification.HttpMethod = GetStringFromJson(reader, "HttpMethod");
                                    notification.ElapsedMilliseconds = GetIntFromJson(reader, "ElapsedMilliseconds");
                                    notification.ResponseStatusCode = GetIntFromJson(reader, "ResponseStatusCode");
                                    notification.TenantName = GetStringFromJson(reader, "TenantName");
                                    notification.CustomInfo = GetStringFromJson(reader, "CustomInfo");
                                    notification.InnerRequestsCount = GetIntFromJson(reader, "InnerRequestsCount");
                                    // notification.QueryTimings = GetRavenJObjectFromJson(reader, "QueryTimings"); // TODO (TrafficWatch) : Handle this both server and client sides

                                    if (config.PrintOutput)
                                        Console.Write("\rRequest #{0} Stored...\t\t ", ++requestsCounter);

                                    var jobj = JObject.FromObject(notification);
                                    jobj.WriteTo(jsonWriter);

                                    if (sp.ElapsedMilliseconds > 5000)
                                    {
                                        streamWriter.Flush();
                                        sp.Restart();
                                    }
                                }
                            }
                            jsonWriter.WriteEndArray();
                            streamWriter.Flush();
                            if (config.IsCompressed)
                                finalStream.Dispose();
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("\r\n\nError while reading messages from server : " + ex);
                }
                finally
                {
                    Console.WriteLine("\r\n\nClosing connection to server...`");
                    try
                    {
                        await
                            client.CloseAsync(WebSocketCloseStatus.NormalClosure, "CLOSE_NORMAL", CancellationToken.None)
                                .ConfigureAwait(false);
                    }
                    catch
                    {
                        // ignored
                    }
                }
            }
        }

        private static string GetStringFromJson(BlittableJsonReaderObject reader, string value)
        {
            string str;
            if (reader.TryGet(value, out str) == false)
                throw new InvalidOperationException($"Missing string '{value}' in message : ${reader}");
            return str;
        }

        private static DateTime GetDateTimeFromJson(BlittableJsonReaderObject reader, string value)
        {
            string str;
            if (reader.TryGet(value, out str) == false)
                throw new InvalidOperationException($"Missing DateTime '{value}' in message : ${reader}");
            return DateTime.Parse(str);
        }

        private static int GetIntFromJson(BlittableJsonReaderObject reader, string value)
        {
            int int32;
            if (reader.TryGet(value, out int32) == false)
                throw new InvalidOperationException($"Missing int '{value}' in message : ${reader}");
            return int32;
        }
    }
}
