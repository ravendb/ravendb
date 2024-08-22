// ------------------------------------------------------------[-----------
//  <copyright file="ChangesHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Net.WebSockets;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.Exceptions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.TrafficWatch
{
    public sealed class TrafficWatchHandler : ServerRequestHandler
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
                    var json = context.ReadObject(TrafficWatchToLog.Instance.ToJson(), "traffic-watch/configuration");
                    writer.WriteObject(json);
                }
            }
        }

        [RavenAction("/admin/traffic-watch/configuration", "POST", AuthorizationStatus.Operator)]
        public async Task SetConfiguration()
        {
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestBodyStream(), "traffic-watch/configuration");

                var configuration = JsonDeserializationServer.Parameters.PutTrafficWatchConfigurationParameters(json);
                if (configuration.Persist)
                    AssertCanPersistConfiguration();

                TrafficWatchToLog.Instance.UpdateConfiguration(configuration);

                if (configuration.Persist)
                {
                    try
                    {
                        using var jsonFileModifier = SettingsJsonModifier.Create(context, ServerStore.Configuration.ConfigPath);
                        jsonFileModifier.SetOrRemoveIfDefault(configuration.TrafficWatchMode, x => x.TrafficWatch.TrafficWatchMode);
                        jsonFileModifier.CollectionSetOrRemoveIfDefault(configuration.Databases, x => x.TrafficWatch.Databases);
                        jsonFileModifier.CollectionSetOrRemoveIfDefault(configuration.StatusCodes, x => x.TrafficWatch.StatusCodes);
                        jsonFileModifier.SetOrRemoveIfDefault(configuration.MinimumResponseSizeInBytes.GetValue(SizeUnit.Bytes), x => x.TrafficWatch.MinimumResponseSize);
                        jsonFileModifier.SetOrRemoveIfDefault(configuration.MinimumRequestSizeInBytes.GetValue(SizeUnit.Bytes), x => x.TrafficWatch.MinimumRequestSize);
                        jsonFileModifier.SetOrRemoveIfDefault(configuration.MinimumDurationInMs, x => x.TrafficWatch.MinimumDuration);
                        jsonFileModifier.CollectionSetOrRemoveIfDefault(configuration.HttpMethods, x => x.TrafficWatch.HttpMethods);
                        jsonFileModifier.CollectionSetOrRemoveIfDefault(configuration.ChangeTypes, x => x.TrafficWatch.ChangeTypes);
                        jsonFileModifier.CollectionSetOrRemoveIfDefault(configuration.CertificateThumbprints, x => x.TrafficWatch.CertificateThumbprints);
                        await jsonFileModifier.ExecuteAsync();
                    }
                    catch (Exception e)
                    {
                        throw new PersistConfigurationException(
                            "The traffic watch logs configuration was modified but couldn't be persistent. The configuration will be reverted on server restart.", e);
                    }
                }

                NoContentStatus();
            }
        }
    }
}
