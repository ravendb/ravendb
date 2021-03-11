using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Replication;
using Raven.Client.ServerWide;
using Raven.Server.Documents.Replication;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class ReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/replication/tombstones", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetAllTombstones()
        {
            var start = GetStart();
            var pageSize = GetPageSize();

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
        }

        [RavenAction("/databases/*/replication/conflicts", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public Task GetReplicationConflicts()
        {
            var docId = GetStringQueryString("docId", required: false);
            var etag = GetLongQueryString("etag", required: false) ?? 0;
            return string.IsNullOrWhiteSpace(docId) ?
                GetConflictsByEtag(etag) :
                GetConflictsForDocument(docId);
        }

        private async Task GetConflictsByEtag(long etag)
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var skip = GetStart();
                var pageSize = GetPageSize();

                var alreadyAdded = new HashSet<LazyStringValue>(LazyStringValueComparer.Instance);
                var array = new DynamicJsonArray();
                var conflicts = Database.DocumentsStorage.ConflictsStorage.GetConflictsAfter(context, etag);
                foreach (var conflict in conflicts)
                {
                    if (alreadyAdded.Add(conflict.Id))
                    {
                        if (skip > 0)
                        {
                            skip--;
                            continue;
                        }
                        if (pageSize-- <= 0)
                            break;
                        array.Add(new DynamicJsonValue
                        {
                            [nameof(GetConflictsResult.Id)] = conflict.Id,
                            [nameof(GetConflictsResult.Conflict.LastModified)] = conflict.LastModified
                        });
                    }
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["TotalResults"] = Database.DocumentsStorage.ConflictsStorage.GetNumberOfDocumentsConflicts(context),
                    [nameof(GetConflictsResult.Results)] = array
                });
            }
        }

        private async Task GetConflictsForDocument(string docId)
        {
            long maxEtag = 0;
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
                        [nameof(GetConflictsResult.Conflict.ChangeVector)] = conflict.ChangeVector,
                        [nameof(GetConflictsResult.Conflict.Doc)] = conflict.Doc,
                        [nameof(GetConflictsResult.Conflict.LastModified)] = conflict.LastModified
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(GetConflictsResult.Id)] = docId,
                    [nameof(GetConflictsResult.LargestEtag)] = maxEtag,
                    [nameof(GetConflictsResult.Results)] = array
                });
            }
        }

        [RavenAction("/databases/*/replication/performance", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Performance()
        {
            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                writer.WriteStartObject();

                writer.WriteArray(context, nameof(ReplicationPerformance.Incoming), Database.ReplicationLoader.IncomingHandlers, (w, c, handler) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(ReplicationPerformance.IncomingStats.Source));
                    w.WriteString(handler.SourceFormatted);
                    w.WriteComma();

                    w.WriteArray(c, nameof(ReplicationPerformance.IncomingStats.Performance), handler.GetReplicationPerformance(), (innerWriter, innerContext, performance) =>
                    {
                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(performance);
                        innerWriter.WriteObject(context.ReadObject(djv, "replication/performance"));
                    });

                    w.WriteEndObject();
                });
                writer.WriteComma();

                var reporters = Database.ReplicationLoader.OutgoingHandlers.Concat<IReportOutgoingReplicationPerformance>(Database.ReplicationLoader
                        .OutgoingConnectionsLastFailureToConnect.Values);

                writer.WriteArray(context, nameof(ReplicationPerformance.Outgoing), reporters, (w, c, handler) =>
                {
                    w.WriteStartObject();

                    w.WritePropertyName(nameof(ReplicationPerformance.OutgoingStats.Destination));
                    w.WriteString(handler.DestinationFormatted);
                    w.WriteComma();

                    w.WriteArray(c, nameof(ReplicationPerformance.OutgoingStats.Performance), handler.GetReplicationPerformance(), (innerWriter, innerContext, performance) =>
                    {
                        var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(performance);
                        innerWriter.WriteObject(context.ReadObject(djv, "replication/performance"));
                    });

                    w.WriteEndObject();
                });

                writer.WriteEndObject();
            }
        }

        [RavenAction("/databases/*/replication/performance/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task PerformanceLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                await using (var ms = new MemoryStream())
                using (var collector = new LiveReplicationPerformanceCollector(Database))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 100);

                    // 2. Send data to webSocket when available
                    while (Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (await collector.SendStatsOrHeartbeatToWebSocket(receive, webSocket, ContextPool, ms, 4000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        [RavenAction("/databases/*/replication/pulses/live", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, SkipUsagesCount = true)]
        public async Task PulsesLive()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                var receiveBuffer = new ArraySegment<byte>(new byte[1024]);
                var receive = webSocket.ReceiveAsync(receiveBuffer, Database.DatabaseShutdown);

                await using (var ms = new MemoryStream())
                using (var collector = new LiveReplicationPulsesCollector(Database))
                {
                    // 1. Send data to webSocket without making UI wait upon opening webSocket
                    await SendPulsesOrHeartbeatToWebSocket(receive, webSocket, collector, ms, 100);

                    // 2. Send data to webSocket when available
                    while (Database.DatabaseShutdown.IsCancellationRequested == false)
                    {
                        if (await SendPulsesOrHeartbeatToWebSocket(receive, webSocket, collector, ms, 4000) == false)
                        {
                            break;
                        }
                    }
                }
            }
        }

        private async Task<bool> SendPulsesOrHeartbeatToWebSocket(Task<WebSocketReceiveResult> receive, WebSocket webSocket,
            LiveReplicationPulsesCollector collector, MemoryStream ms, int timeToWait)
        {
            if (receive.IsCompleted || webSocket.State != WebSocketState.Open)
                return false;

            var tuple = await collector.Pulses.TryDequeueAsync(TimeSpan.FromMilliseconds(timeToWait));
            if (tuple.Item1 == false)
            {
                await webSocket.SendAsync(WebSocketHelper.Heartbeat, WebSocketMessageType.Text, true, Database.DatabaseShutdown);
                return true;
            }

            ms.SetLength(0);

            using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
            {
                var pulse = tuple.Item2;
                context.Write(writer, pulse.ToJson());
            }

            ms.TryGetBuffer(out ArraySegment<byte> bytes);
            await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, Database.DatabaseShutdown);

            return true;
        }

        [RavenAction("/databases/*/replication/active-connections", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetReplicationActiveConnections()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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
                        ["Disabled"] = item.Disabled
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["IncomingConnections"] = incoming,
                    ["OutgoingConnections"] = outgoing
                });
            }
        }

        [RavenAction("/databases/*/replication/debug/outgoing-failures", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationOutgoingFailureStats()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.ReplicationLoader.OutgoingFailureInfo)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            [nameof(item.Key.Url)] = item.Key.Url,
                            [nameof(item.Key.Database)] = item.Key.Database,
                            [nameof(item.Key.Disabled)] = item.Key.Disabled
                        },
                        ["Value"] = new DynamicJsonValue
                        {
                            ["ErrorsCount"] = item.Value.Errors.Count,
                            [nameof(item.Value.Errors)] = new DynamicJsonArray(item.Value.Errors.Select(e => e.ToString())),
                            [nameof(item.Value.NextTimeout)] = item.Value.NextTimeout,
                            [nameof(item.Value.RetryOn)] = item.Value.RetryOn,
                            [nameof(item.Value.External)] = item.Value.External,
                            [nameof(item.Value.DestinationDbId)] = item.Value.DestinationDbId,
                            [nameof(item.Value.LastHeartbeatTicks)] = item.Value.LastHeartbeatTicks,
                        }
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Stats"] = data
                });
            }
        }

        [RavenAction("/databases/*/replication/debug/incoming-last-activity-time", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationIncomingActivityTimes()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

                context.Write(writer, new DynamicJsonValue
                {
                    ["Stats"] = data
                });
            }
        }

        [RavenAction("/databases/*/replication/debug/incoming-rejection-info", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationIncomingRejectionInfo()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var stats = new DynamicJsonArray();
                foreach (var statItem in Database.ReplicationLoader.IncomingRejectionStats)
                {
                    stats.Add(new DynamicJsonValue
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

                context.Write(writer, new DynamicJsonValue
                {
                    ["Stats"] = stats
                });
            }
        }

        [RavenAction("/databases/*/replication/debug/outgoing-reconnect-queue", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, IsDebugInformationEndpoint = true)]
        public async Task GetReplicationReconnectionQueue()
        {
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var queueItem in Database.ReplicationLoader.ReconnectQueue)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Url"] = queueItem.Url,
                        ["Database"] = queueItem.Database,
                        ["Disabled"] = queueItem.Disabled
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["Queue-Info"] = data
                });
            }
        }

        [RavenAction("/databases/*/studio-tasks/suggest-conflict-resolution", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task SuggestConflictResolution()
        {
            var docId = GetQueryStringValueAndAssertIfSingleAndNotEmpty("docId");
            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            using (context.OpenReadTransaction())
            {
                var conflicts = context.DocumentDatabase.DocumentsStorage.ConflictsStorage.GetConflictsFor(context, docId);
                var advisor = new ConflictResolverAdvisor(conflicts.Select(c => c.Doc), context);
                var resolved = advisor.Resolve();

                context.Write(writer, new DynamicJsonValue
                {
                    [nameof(ConflictResolverAdvisor.MergeResult.Document)] = resolved.Document,
                    [nameof(ConflictResolverAdvisor.MergeResult.Metadata)] = resolved.Metadata
                });
            }
        }

        [RavenAction("/databases/*/replication/conflicts/solver", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRevisionsConfig()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            using (context.OpenReadTransaction())
            {
                ConflictSolver solverConfig;
                using (var rawRecord = Server.ServerStore.Cluster.ReadRawDatabaseRecord(context, Database.Name))
                {
                    solverConfig = rawRecord?.ConflictSolverConfiguration;
                }

                if (solverConfig != null)
                {
                    var resolveByCollection = new DynamicJsonValue();
                    foreach (var collection in solverConfig.ResolveByCollection)
                    {
                        resolveByCollection[collection.Key] = collection.Value.ToJson();
                    }

                    await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(solverConfig.ResolveToLatest)] = solverConfig.ResolveToLatest,
                            [nameof(solverConfig.ResolveByCollection)] = resolveByCollection
                        });
                    }
                }
                else
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                }
            }
        }
    }
}
