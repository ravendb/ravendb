using System;
using System.IO;
using System.Threading;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Tcp
{
    public class TcpNegotiation
    {
        public static TcpConnectionHeaderMessage.SupportedFeatures NegotiateProtocolVersion(JsonOperationContext context, Stream stream, TcpNegotiateParamaters parameters)
        {
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                var currentVersion = parameters.Version;
                while (true)
                {
                    if (parameters.CancellationToken.IsCancellationRequested)
                        throw new OperationCanceledException($"Stopped TCP negotiation for {parameters.Operation} because of cancellation request");

                    SendTcpVersionInfo(context, writer, parameters, currentVersion);
                    var version = parameters.ReadResponseAndGetVersionCallback(context, writer, stream, parameters.Url);
                    //In this case we usually throw internally but for completeness we better handle it
                    if (version == -2)
                    {
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Drop, TcpConnectionHeaderMessage.DropBaseLine);
                    }
                    var status = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version, out var prevSupported);
                    if (status == TcpConnectionHeaderMessage.SupportedStatus.OutOfRange)
                    {
                        throw new ArgumentException($"The {parameters.Operation} version {parameters.Version} is out of range, our lowest version is {prevSupported}");
                    }

                    if (status == TcpConnectionHeaderMessage.SupportedStatus.Supported)
                    {
                        // if we had a negotiation we need to notify the other side about the current supported version.
                        if (currentVersion != version)
                        {
                            SendTcpVersionInfo(context, writer, parameters, currentVersion);
                        }
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(parameters.Operation, version);
                    }

                    currentVersion = prevSupported;
                }
            }
        }

        private static void SendTcpVersionInfo(JsonOperationContext context, BlittableJsonTextWriter writer, TcpNegotiateParamaters parameters, int currentVersion)
        {
            context.Write(writer, new DynamicJsonValue
            {
                [nameof(TcpConnectionHeaderMessage.DatabaseName)] = parameters.Database,
                [nameof(TcpConnectionHeaderMessage.Operation)] = parameters.Operation.ToString(),
                [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = parameters.NodeTag,
                [nameof(TcpConnectionHeaderMessage.OperationVersion)] = currentVersion
            });
            writer.Flush();
        }
    }

    public class TcpNegotiateParamaters
    {
        public TcpConnectionHeaderMessage.OperationTypes Operation { get; set; }
        public int Version { get; set; }
        public string Database { get; set; }
        public string NodeTag { get; set; }
        public string Url { get; set; }
        public CancellationToken CancellationToken { get; set; }

        /// <summary>
        /// ReadResponseAndGetVersion Function should take care reading the TcpConnectionHeaderResponse respond from the input 'stream'
        /// And return the version of the supported TCP protocol.
        ///
        /// If the respond is 'Drop' the function should throw.
        /// If the respond is 'None' the function should throw.
        /// If the respond is 'TcpMissMatch' the function should return the read version.
        /// </summary>
        public Func<JsonOperationContext, BlittableJsonTextWriter, Stream, string, int> ReadResponseAndGetVersionCallback { get; set; }
    }
}
