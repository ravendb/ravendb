using System;
using System.IO;
using System.Threading;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Client.ServerWide.Tcp
{
    public class TcpNegotiation
    {
        public const int OutOfRangeStatus = -1;
        public const int DropStatus = -2;

        private static readonly Logger Log = LoggingSource.Instance.GetLogger<TcpNegotiation>("TCP Negotiation"); 

        public static TcpConnectionHeaderMessage.SupportedFeatures NegotiateProtocolVersion(JsonOperationContext context, Stream stream, TcpNegotiateParameters parameters)
        {
            if (Log.IsInfoEnabled)
            {
                Log.Info($"Start negotiation for {parameters.Operation} operation with {parameters.DestinationNodeTag ?? parameters.DestinationUrl}");
            }

            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                var current = parameters.Version;
                while (true)
                {
                    if (parameters.CancellationToken.IsCancellationRequested)
                        throw new OperationCanceledException($"Stopped TCP negotiation for {parameters.Operation} because of cancellation request");

                    SendTcpVersionInfo(context, writer, parameters, current);
                    var version = parameters.ReadResponseAndGetVersionCallback(context, writer, stream, parameters.DestinationUrl);
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"Read response from {parameters.SourceNodeTag ?? parameters.DestinationUrl} for '{parameters.Operation}', received version is '{version}'");
                    }

                    if (version == current)
                        break;

                    //In this case we usually throw internally but for completeness we better handle it
                    if (version == DropStatus)
                    {
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Drop, TcpConnectionHeaderMessage.DropBaseLine);
                    }
                    var status = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version, out current);
                    if (status == TcpConnectionHeaderMessage.SupportedStatus.OutOfRange)
                    {
                        SendTcpVersionInfo(context, writer, parameters, OutOfRangeStatus);
                        throw new ArgumentException($"The {parameters.Operation} version {parameters.Version} is out of range, our lowest version is {current}");
                    }
                    if (Log.IsInfoEnabled)
                    {
                        Log.Info($"The version {version} is {status}, will try to agree on '{current}' for {parameters.Operation} with {parameters.DestinationNodeTag ?? parameters.DestinationUrl}.");
                    }
                }
                if (Log.IsInfoEnabled)
                {
                    Log.Info($"{parameters.DestinationNodeTag ?? parameters.DestinationUrl} agreed on version '{current}' for {parameters.Operation}.");
                }
                return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(parameters.Operation, current);
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
                [nameof(TcpConnectionHeaderMessage.AuthorizeInfo)] = parameters.AuthorizeInfo?.ToJson()
            });
            writer.Flush();
        }
    }

    public class TcpNegotiateParameters
    {
        public TcpConnectionHeaderMessage.OperationTypes Operation { get; set; }
        public TcpConnectionHeaderMessage.AuthorizationInfo AuthorizeInfo { get; set; }
        public int Version { get; set; }
        public string Database { get; set; }
        public string SourceNodeTag { get; set; }
        public string DestinationNodeTag { get; set; }
        public string DestinationUrl { get; set; }
        public CancellationToken CancellationToken { get; set; }

        /// <summary>
        /// ReadResponseAndGetVersion Function should take care reading the TcpConnectionHeaderResponse respond from the input 'stream'
        /// And return the version of the supported TCP protocol.
        ///
        /// If the respond is 'Drop' the function should throw.
        /// If the respond is 'None' the function should throw.
        /// If the respond is 'TcpMismatch' the function should return the read version.
        /// </summary>
        public Func<JsonOperationContext, BlittableJsonTextWriter, Stream, string, int> ReadResponseAndGetVersionCallback { get; set; }
    }
}
