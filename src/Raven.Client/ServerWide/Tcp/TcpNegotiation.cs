using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Client.ServerWide.Tcp
{
    public class TcpNegotiation
    {
        public static TcpConnectionHeaderMessage.SupportedFeatures NegotiateProtocolVersion(JsonOperationContext documentsContext, Stream stream, TcpNegotiateParamaters parameters)
        {
            using (var writer = new BlittableJsonTextWriter(documentsContext, stream))
            {
                var currentVersion = parameters.Version;
                while (true)
                {
                    documentsContext.Write(writer, new DynamicJsonValue
                    {
                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = parameters.Database, // _parent.Database.Name,
                        [nameof(TcpConnectionHeaderMessage.Operation)] = parameters.Operation.ToString(),
                        [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = parameters.NodeTag,
                        [nameof(TcpConnectionHeaderMessage.OperationVersion)] = currentVersion
                    });
                    writer.Flush();
                    var version = parameters.ReadRespondAndGetVersion(documentsContext, writer, stream, parameters.Url);
                    //In this case we usally throw internaly but for completeness we better handle it
                    if (version == -2)
                    {
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Drop, TcpConnectionHeaderMessage.DropBaseLine4000);
                    }
                    var (supported, prevSupported) = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version);
                    if (supported)
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(parameters.Operation, version);
                    if (prevSupported == -1)
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.None, TcpConnectionHeaderMessage.NoneBaseLine4000);
                    currentVersion = prevSupported;
                }
            }
        }

        public static async Task<TcpConnectionHeaderMessage.SupportedFeatures> NegotiateProtocolVersionAsync(JsonOperationContext documentsContext, Stream stream, TcpNegotiateParamaters parameters)
        {            
            using (var writer = new BlittableJsonTextWriter(documentsContext, stream))
            {
                var currentVersion = parameters.Version;
                while (true)
                {
                    if(parameters.CancellationToken.IsCancellationRequested)
                        throw new OperationCanceledException($"Stoped Tcp negotiation for {parameters.Operation} because of cancelation request");

                    documentsContext.Write(writer, new DynamicJsonValue
                    {
                        [nameof(TcpConnectionHeaderMessage.DatabaseName)] = parameters.Database, // _parent.Database.Name,
                        [nameof(TcpConnectionHeaderMessage.Operation)] = parameters.Operation.ToString(),
                        [nameof(TcpConnectionHeaderMessage.SourceNodeTag)] = parameters.NodeTag,
                        [nameof(TcpConnectionHeaderMessage.OperationVersion)] = currentVersion
                    });
                    writer.Flush();
                    int version;
                    if (parameters.ReadRespondAndGetVersionAsync == null)
                    {
                        version = parameters.ReadRespondAndGetVersion(documentsContext, writer, stream, parameters.Url);
                    }
                    else
                    {
                        version = await parameters.ReadRespondAndGetVersionAsync(documentsContext, writer, stream, parameters.Url, parameters.CancellationToken).ConfigureAwait(false);                        
                    }
                    //In this case we usally throw internaly but for completeness we better handle it
                    if (version == -2)
                    {
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.Drop, TcpConnectionHeaderMessage.DropBaseLine4000);
                    }
                    var (supported, prevSupported) = TcpConnectionHeaderMessage.OperationVersionSupported(parameters.Operation, version);
                    if (supported)
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(parameters.Operation, version);
                    if (prevSupported == -1)
                        return TcpConnectionHeaderMessage.GetSupportedFeaturesFor(TcpConnectionHeaderMessage.OperationTypes.None, TcpConnectionHeaderMessage.NoneBaseLine4000);
                    currentVersion = prevSupported;
                }
            }
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

        public Func<JsonOperationContext, BlittableJsonTextWriter,Stream,string, int> ReadRespondAndGetVersion { get; set; }
        public Func<JsonOperationContext, BlittableJsonTextWriter, Stream, string, CancellationToken, Task<int>> ReadRespondAndGetVersionAsync { get; set; }
    }
}
