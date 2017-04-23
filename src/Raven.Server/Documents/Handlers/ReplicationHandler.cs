using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Replication;
using Raven.Client.Documents.Replication.Messages;
using Raven.Server.Documents.Replication;
using Raven.Server.Json;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Handlers
{
    public class ReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/replication/tombstones", "GET", "/databases/{databaseName:string}/replication/tombstones?start={start:int}&take={take:int}")]
        public Task GetAllTombstones()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var array = new DynamicJsonArray();
                var tombstones = context.DocumentDatabase.DocumentsStorage.GetTombstonesFrom(context, 0, start, pageSize);
                foreach (var tombstone in tombstones)
                {
                    array.Add(tombstone.ToJson());
                }
                context.Write(writer, new DynamicJsonValue
                {
                    ["Results"] = array
                });
            }

            return Task.CompletedTask;
        }

        //get conflicts for specified document
        [RavenAction("/databases/*/replication/conflicts", "GET", "/databases/{databaseName:string}/replication/conflicts?docId={documentId:string}")]
        public Task GetReplicationConflictsById()
        {
            var docId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");
            DocumentsOperationContext context;
            long maxEtag = 0;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var array = new DynamicJsonArray();
                var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, docId);
                foreach (var conflict in conflicts)
                {
                    if (maxEtag < conflict.Etag)
                        maxEtag = conflict.Etag;

                    array.Add(new DynamicJsonValue
                    {
                        [nameof(GetConflictsResult.Conflict.ChangeVector)] = conflict.ChangeVector.ToJson(),
                        [nameof(GetConflictsResult.Conflict.Doc)] = conflict.Doc
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(GetConflictsResult.Key)] = docId,
                    [nameof(GetConflictsResult.LargestEtag)] = maxEtag,
                    [nameof(GetConflictsResult.Results)] = array
                });

                return Task.CompletedTask;
            }
        }

        [RavenAction("/databases/*/replication/performance", "GET")]
        public Task Performance()
        {
            JsonOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WritePropertyName(nameof(ReplicationPerformance.Incoming));
                writer.WriteArray(context, Database.ReplicationLoader.IncomingHandlers, (w, c, handler) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(ReplicationPerformance.IncomingStats.Source));
                    w.WriteString(handler.SourceFormatted);
                    w.WriteComma();

                    w.WritePropertyName(nameof(ReplicationPerformance.IncomingStats.Performance));
                    w.WriteArray(c, handler.GetReplicationPerformance(), (innerWriter, innerContext, performance) =>
                    {
                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(performance);
                        innerWriter.WriteObject(context.ReadObject(djv, "replication/performance"));
                    });

                    w.WriteEndObject();
                });
                writer.WriteComma();

                writer.WritePropertyName(nameof(ReplicationPerformance.Outgoing));
                writer.WriteArray(context, Database.ReplicationLoader.OutgoingHandlers, (w, c, handler) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(ReplicationPerformance.OutgoingStats.Destination));
                    w.WriteString(handler.DestinationFormatted);
                    w.WriteComma();

                    w.WritePropertyName(nameof(ReplicationPerformance.OutgoingStats.Performance));
                    w.WriteArray(c, handler.GetReplicationPerformance(), (innerWriter, innerContext, performance) =>
                    {
                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(performance);
                        innerWriter.WriteObject(context.ReadObject(djv, "replication/performance"));
                    });

                    w.WriteEndObject();
                });

                writer.WriteEndObject();
            }

            return Task.CompletedTask;
        }


        [RavenAction("/databases/*/replication/performance/live", "GET", SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                using (var ms = new MemoryStream())
                using (var collector = new LiveReplicationPerformanceCollector(Database))
                {
                    while (Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (receive.IsCompleted || webSocket.State != WebSocketState.Open)
                            break;

                        var tuple = await collector.Stats.TryDequeueAsync(TimeSpan.FromSeconds(4));
                        if (tuple.Item1 == false)
                        {
                            await webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, Database.DatabaseShutdown);
                            continue;
                        }

                        ms.SetLength(0);

                        JsonOperationContext context;
                        using (ContextPool.AllocateOperationContext(out context))
                        using (var writer = new BlittableJsonTextWriter(context, ms))
                        {
                            writer.WriteArray(context, tuple.Item2, (w, c, p) =>
                            {
                                p.Write(c, w);
                            });
                        }

                        ArraySegment<byte> bytes;
                        ms.TryGetBuffer(out bytes);

                        await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, Database.DatabaseShutdown);
                    }
                }
            }
        }

        [RavenAction("/databases/*/replication/topology", "GET")]
        public Task GetReplicationTopology()
        {
            // TODO: Remove this, use "/databases/*/topology" isntead
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/active-connections", "GET")]
        public Task GetReplicationActiveConnections()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var incoming = new DynamicJsonArray();
                foreach (var item in Database.ReplicationLoader.IncomingConnections)
                {
                    incoming.Add(new DynamicJsonValue
                    {
                        ["SourceDatabaseId"] = item.SourceDatabaseId,
                        ["SourceDatabaseName"] = item.SourceDatabaseName,
                        ["SourceMachineName"] = item.SourceMachineName,
                        ["SourceUrl"] = item.SourceUrl
                    });
                }

                var outgoing = new DynamicJsonArray();
                foreach (var item in Database.ReplicationLoader.OutgoingConnections)
                {
                    outgoing.Add(new DynamicJsonValue
                    {
                        ["Url"] = item.Url,
                        ["Database"] = item.Database,
                        ["Disabled"] = item.Disabled,
                        ["IgnoredClient"] = item.IgnoredClient,
                        ["SkipIndexReplication"] = item.SkipIndexReplication,
                        ["SpecifiedCollections"] = item.SpecifiedCollections
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["IncomingConnections"] = incoming,
                    ["OutgoingConnections"] = outgoing
                });
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/outgoing-failures", "GET")]
        public Task GetReplicationOugoingFailureStats()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.ReplicationLoader.OutgoingFailureInfo)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["Url"] = item.Key.Url,
                            ["Database"] = item.Key.Database,
                            ["Disabled"] = item.Key.Disabled,
                            ["IgnoredClient"] = item.Key.IgnoredClient,
                            ["SkipIndexReplication"] = item.Key.SkipIndexReplication,
                            ["SpecifiedCollections"] = item.Key.SpecifiedCollections
                        },
                        ["Value"] = new DynamicJsonValue
                        {
                            ["ErrorCount"] = item.Value.ErrorCount,
                            ["NextTimout"] = item.Value.NextTimout
                        }
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/incoming-last-activity-time", "GET")]
        public Task GetReplicationIncomingActivityTimes()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.ReplicationLoader.IncomingLastActivityTime)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["SourceDatabaseId"] = item.Key.SourceDatabaseId,
                            ["SourceDatabaseName"] = item.Key.SourceDatabaseName,
                            ["SourceMachineName"] = item.Key.SourceMachineName,
                            ["SourceUrl"] = item.Key.SourceUrl
                        },
                        ["Value"] = item.Value
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/incoming-rejection-info", "GET")]
        public Task GetReplicationIncomingRejectionInfo()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var statItem in Database.ReplicationLoader.IncomingRejectionStats)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["SourceDatabaseId"] = statItem.Key.SourceDatabaseId,
                            ["SourceDatabaseName"] = statItem.Key.SourceDatabaseName,
                            ["SourceMachineName"] = statItem.Key.SourceMachineName,
                            ["SourceUrl"] = statItem.Key.SourceUrl
                        },
                        ["Value"] = new DynamicJsonArray(statItem.Value.Select(x => new DynamicJsonValue
                        {
                            ["Reason"] = x.Reason,
                            ["When"] = x.When
                        }))
                    });
                }

                context.Write(writer, data);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/outgoing-reconnect-queue", "GET")]
        public Task GetReplicationReconnectionQueue()
        {
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var queueItem in Database.ReplicationLoader.ReconnectQueue)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Url"] = queueItem.Url,
                        ["Database"] = queueItem.Database,
                        ["Disabled"] = queueItem.Disabled,
                        ["IgnoredClient"] = queueItem.IgnoredClient,
                        ["SkipIndexReplication"] = queueItem.SkipIndexReplication,
                        ["SpecifiedCollections"] = queueItem.SpecifiedCollections
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/studio-tasks/suggest-conflict-resolution", "GET", "/databases/{databaseName:string}/studio-tasks/suggest-conflict-resolution?docId={documentId:string}")]
        public Task SuggestConflictResolution()
        {
            var docId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, docId);
                var advisor = new ConflictResovlerAdvisor(conflicts.Select(c => c.Doc), context);
                var resovled = advisor.Resolve();

                context.Write(writer, new DynamicJsonValue
                {
                    ["Document"] = resovled.Document,
                    ["Metadata"] = resovled.Metadata
                });

                return Task.CompletedTask;
            }
        }
    }
}