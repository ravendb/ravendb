using System;
using System.IO;
using Raven.Client.ServerWide.Tcp;
using Raven.Server.Logging;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.Tcp.Sync
{
    internal static class TcpNegotiationSyncExtensions
    {
        private static readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForServer(typeof(TcpNegotiationSyncExtensions));

        internal static TcpConnectionHeaderMessage.SupportedFeatures NegotiateProtocolVersion(this TcpNegotiation.SyncTcpNegotiation syncTcpNegotiation, JsonOperationContext context, Stream stream, TcpNegotiateParameters parameters)
        {
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"Start negotiation for {parameters.Operation} operation with {parameters.DestinationNodeTag ?? parameters.DestinationUrl}");
            }

            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                var current = parameters.Version;
                bool dataCompression;
                while (true)
                {
                    if (parameters.CancellationToken.IsCancellationRequested)
                        throw new OperationCanceledException($"Stopped TCP negotiation for {parameters.Operation} because of cancellation request");

                    SendTcpVersionInfo(context, writer, parameters, current);
                    var response = parameters.ReadResponseAndGetVersionCallback(context, writer, stream, parameters.DestinationUrl);
                    var version = response.Version;
                    dataCompression = response.LicensedFeatures?.DataCompression ?? false;

                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"Read response from {parameters.SourceNodeTag ?? parameters.DestinationUrl} for '{parameters.Operation}', received version is '{version}'");
                    }

                    if (version == current)
                        break;

                    //In this case we usually throw internally but for completeness we better handle it
                    if (version == TcpNegotiation.DropStatus)
                    {
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Drop, TcpConnectionHeaderMessage.DropBaseLine);
                    }
                    var status = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version, out current);
                    if (status == TcpConnectionHeaderMessage.SupportedStatus.OutOfRange)
                    {
                        SendTcpVersionInfo(context, writer, parameters, TcpNegotiation.OutOfRangeStatus);
                        throw new ArgumentException($"The {parameters.Operation} version {parameters.Version} is out of range, our lowest version is {current}");
                    }
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"The version {version} is {status}, will try to agree on '{current}' for {parameters.Operation} with {parameters.DestinationNodeTag ?? parameters.DestinationUrl}.");
                    }
                }
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"{parameters.DestinationNodeTag ?? parameters.DestinationUrl} agreed on version '{current}' for {parameters.Operation}.");
                }

                var supportedFeatures = TcpConnectionHeaderMessage.GetSupportedFeaturesFor(parameters.Operation, current);

                return new TcpConnectionHeaderMessage.SupportedFeatures(supportedFeatures)
                {
                    DataCompression = dataCompression
                };
            }
        }

        private static void SendTcpVersionInfo(JsonOperationContext context, BlittableJsonTextWriter writer, TcpNegotiateParameters parameters, int currentVersion)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info($"Send negotiation for {parameters.Operation} in version {currentVersion}");
            }

            context.Write(writer, new DynamicJsonValue
            {
                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = parameters.Database,
                [nameof(TcpConnectionHeaderMessage.Operation)] = parameters.Operation.ToString(),
                [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = parameters.SourceNodeTag,
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = currentVersion,
                [nameof(TcpConnectionHeaderMessage.AuthorizeInfo)] = parameters.AuthorizeInfo?.ToJson(),
                [nameof(TcpConnectionHeaderMessage.ServerId)] = parameters.DestinationServerId,
                [nameof(TcpConnectionHeaderMessage.LicensedFeatures)] = parameters.LicensedFeatures?.ToJson()

            });
            writer.Flush();
        }
    }
}
