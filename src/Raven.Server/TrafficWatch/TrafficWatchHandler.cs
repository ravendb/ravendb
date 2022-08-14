// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading.Tasks;
using NuGet.Protocol;
using Raven.Client.ServerWide.Operations.Logs;
using Raven.Server.Config.Categories;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.TrafficWatch
{
    public class TrafficWatchHandler : RequestHandler
    {
        private static readonly Logger _logger = LoggingSource.Instance.GetLogger<TrafficWatchHandler>("Server");

        [RavenAction("/admin/traffic-watch", "GET", AuthorizationStatus.Operator)]
        public async Task TrafficWatchWebsockets()
        {
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            {
                using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    try
                    {
                        var resourceName = GetStringQueryString("resourceName", required: false);
                        var connection = new TrafficWatchConnection(webSocket, resourceName, context, ServerStore.ServerShutdown);
                        TrafficWatchManager.AddConnection(connection);
                        await connection.StartSendingNotifications();
                    }
                    catch (IOException)
                    {
                        // nothing to do - connection closed
                    }
                    catch (Exception ex)
                    {
                        if (_logger.IsInfoEnabled)
                            _logger.Info("Error encountered in TrafficWatch handler", ex);

                        try
                        {
                            await using (var ms = new MemoryStream())
                            {
                                await using (var writer = new AsyncBlittableJsonTextWriter(context, ms))
                                {
                                    context.Write(writer, new DynamicJsonValue
                                    {
                                        ["Exception"] = ex
                                    });
                                }

                                ms.TryGetBuffer(out ArraySegment<byte> bytes);
                                await webSocket.SendAsync(bytes, WebSocketMessageType.Text, true, ServerStore.ServerShutdown);
                            }
                        }
                        catch (Exception)
                        {
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Failed to send the error in TrafficWatch handler to the client", ex);
                        }
                    }
                }
            }
        }

        [RavenAction("/admin/traffic-watch/configuration", "GET", AuthorizationStatus.Operator)]
        public async Task GetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var djv = new DynamicJsonValue
                    {
                        [nameof(TrafficWatchConfiguration.TrafficWatchMode)] = TrafficWatchToLog.Instance.Configuration.TrafficWatchMode,
                        [nameof(TrafficWatchConfiguration.Databases)] =
                            new DynamicJsonArray(TrafficWatchToLog.Instance.Configuration.Databases ?? new HashSet<string>()),
                        [nameof(TrafficWatchConfiguration.StatusCodes)] =
                            new DynamicJsonArray(TrafficWatchToLog.Instance.Configuration.StatusCodes ?? new HashSet<int>()),
                        [nameof(TrafficWatchConfiguration.MinimumResponseSize)] =
                            TrafficWatchToLog.Instance.Configuration.MinimumResponseSize.GetValue(SizeUnit.Bytes),
                        [nameof(TrafficWatchConfiguration.MinimumRequestSize)] = TrafficWatchToLog.Instance.Configuration.MinimumRequestSize.GetValue(SizeUnit.Bytes),
                        [nameof(TrafficWatchConfiguration.MinimumDuration)] = TrafficWatchToLog.Instance.Configuration.MinimumDuration,
                        [nameof(TrafficWatchConfiguration.HttpMethods)] =
                            new DynamicJsonArray(TrafficWatchToLog.Instance.Configuration.HttpMethods ?? new HashSet<string>()),
                        [nameof(TrafficWatchConfiguration.ChangeTypes)] =
                            new DynamicJsonArray(TrafficWatchToLog.Instance.Configuration.ChangeTypes?.Select(x => x.ToString()) ?? new string[]{})
                    };

                    var json = context.ReadObject(djv, "traffic-watch/configuration");
                    writer.WriteObject(json);
                }
            }
        }
    }
}
